// Copyright 2016-2021:
//   GobySoft, LLC (2013-)
//   Community contributors (see AUTHORS file)
// File authors:
//   Toby Schneider <toby@gobysoft.org>
//
//
// This file is part of the Goby Underwater Autonomy Project Libraries
// ("The Goby Libraries").
//
// The Goby Libraries are free software: you can redistribute them and/or modify
// them under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or
// (at your option) any later version.
//
// The Goby Libraries are distributed in the hope that they will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Goby.  If not, see <http://www.gnu.org/licenses/>.

#ifndef GOBY_MIDDLEWARE_TRANSPORT_DETAIL_SUBSCRIPTION_STORE_H
#define GOBY_MIDDLEWARE_TRANSPORT_DETAIL_SUBSCRIPTION_STORE_H

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <thread>
#include <typeindex>
#include <unordered_map>
#include <vector>

#include "goby/middleware/transport/publisher.h"

namespace goby
{
namespace middleware
{
namespace detail
{
/// \brief Base class for interthread subscription information. Non-template so it can be stored in a single container. Used by InterThreadTransporter
class SubscriptionStoreBase
{
  private:
    // for each thread, stores a map of Datas to SubscriptionStores so that can call poll() on all the stores
    using StoresMap = std::unordered_map<std::type_index, std::shared_ptr<SubscriptionStoreBase>>;
    static std::unordered_map<std::thread::id, StoresMap> stores_;
    static std::shared_timed_mutex stores_mutex_;

  public:
    SubscriptionStoreBase() = default;
    virtual ~SubscriptionStoreBase() = default;

    // returns number of data items posted to callbacks
    static int poll_all(std::thread::id thread_id,
                        std::unique_ptr<std::unique_lock<std::timed_mutex>>& lock)
    {
        // make a copy so that other threads can subscribe if
        // necessary in their callbacks
        StoresMap stores;
        {
            std::shared_lock<std::shared_timed_mutex> stores_lock(stores_mutex_);
            if (stores_.count(thread_id))
                stores = stores_.at(thread_id);
        }

        int poll_items = 0;
        for (auto const& s : stores) poll_items += s.second->poll(thread_id, lock);
        return poll_items;
    }

    static void unsubscribe_all(std::thread::id thread_id)
    {
        std::shared_lock<std::shared_timed_mutex> stores_lock(stores_mutex_);
        if (stores_.count(thread_id))
        {
            for (auto const& s : stores_.at(thread_id)) s.second->unsubscribe_all_groups(thread_id);
        }
    }

    static void remove(std::thread::id thread_id)
    {
        std::lock_guard<decltype(stores_mutex_)> lock(stores_mutex_);
        stores_.erase(thread_id);
    }

  protected:
    template <typename StoreType> static void insert(std::thread::id thread_id)
    {
        // check the store, and if there isn't one for this type, create one
        std::lock_guard<decltype(stores_mutex_)> lock(stores_mutex_);

        if (!stores_.count(thread_id))
            stores_.insert(std::make_pair(thread_id, StoresMap()));

        auto index = std::type_index(typeid(StoreType));
        if (!stores_.at(thread_id).count(index))
            stores_.at(thread_id).insert(
                std::make_pair(index, std::shared_ptr<StoreType>(new StoreType)));
    }

  protected:
    virtual int poll(std::thread::id thread_id,
                     std::unique_ptr<std::unique_lock<std::timed_mutex>>& lock) = 0;
    virtual void unsubscribe_all_groups(std::thread::id thread_id) = 0;
};

struct DataProtection
{
    DataProtection(std::shared_ptr<std::mutex> dm, std::shared_ptr<std::condition_variable_any> pcv,
                   std::shared_ptr<std::timed_mutex> pm)
        : data_mutex(dm), poller_cv(pcv), poller_mutex(pm)
    {
    }

    std::shared_ptr<std::mutex> data_mutex;
    std::shared_ptr<std::condition_variable_any> poller_cv;
    std::shared_ptr<std::timed_mutex> poller_mutex;
};

/// \brief Storage class for a specific interthread subscription (and related data). Used by InterThreadTransporter
template <typename Data> class SubscriptionStore : public SubscriptionStoreBase
{
  public:
    static void subscribe(std::function<void(std::shared_ptr<const Data>)> func, const Group& group,
                          std::thread::id thread_id, std::shared_ptr<std::mutex> data_mutex,
                          std::shared_ptr<std::condition_variable_any> cv,
                          std::shared_ptr<std::timed_mutex> poller_mutex)
    {
        {
            std::lock_guard<std::shared_timed_mutex> lock(subscription_mutex_);

            // insert callback
            auto it =
                subscription_callbacks_.insert(std::make_pair(thread_id, Callback(group, func)));
            // insert group with iterator to callback
            subscription_groups_.insert(std::make_pair(group, it));

            // if necessary, create a DataQueue for this thread
            auto queue_it = data_.find(thread_id);
            if (queue_it == data_.end())
            {
                auto bool_it_pair = data_.insert(std::make_pair(thread_id, DataQueue()));
                queue_it = bool_it_pair.first;
            }
            queue_it->second.create(group);

            // if we don't have a condition variable already for this thread, store it
            if (!data_protection_.count(thread_id))
                data_protection_.insert(std::make_pair(
                    thread_id, detail::DataProtection(data_mutex, cv, poller_mutex)));
        }

        // try inserting a copy of this templated class via the base class for SubscriptionStoreBase::poll_all to use
        SubscriptionStoreBase::insert<SubscriptionStore<Data>>(thread_id);
    }

    static void unsubscribe(const Group& group, std::thread::id thread_id)
    {
        {
            std::lock_guard<std::shared_timed_mutex> lock(subscription_mutex_);

            // iterate over subscriptions for this group, and erase the ones belonging to this thread_id
            auto range = subscription_groups_.equal_range(group);
            for (auto it = range.first; it != range.second;)
            {
                auto sub_thread_id = it->second->first;

                if (sub_thread_id == thread_id)
                {
                    subscription_callbacks_.erase(it->second);
                    it = subscription_groups_.erase(it);
                }
                else
                {
                    ++it;
                }
            }

            // remove the dataqueue for this group
            auto queue_it = data_.find(thread_id);
            queue_it->second.remove(group);
        }
    }

    static void publish(std::shared_ptr<const Data> data, const Group& group,
                        const Publisher<Data>& publisher)
    {
        // push new data
        // build up local vector of relevant condition variables while locked
        std::vector<detail::DataProtection> cv_to_notify;
        {
            std::shared_lock<std::shared_timed_mutex> lock(subscription_mutex_);

            auto range = subscription_groups_.equal_range(group);
            for (auto it = range.first; it != range.second; ++it)
            {
                std::thread::id thread_id = it->second->first;

                // don't store a copy if publisher == subscriber, and echo is false
                if (thread_id != std::this_thread::get_id() || publisher.cfg().echo())
                {
                    // protect the DataQueue we are writing to
                    std::unique_lock<std::mutex> lock(*(data_protection_.at(thread_id).data_mutex));
                    auto queue_it = data_.find(thread_id);
                    queue_it->second.insert(group, data);
                    cv_to_notify.push_back(data_protection_.at(thread_id));
                }
            }
        }

        // unlock and notify condition variables from local vector
        for (const auto& data_protection : cv_to_notify)
        {
            {
                // lock to ensure the other thread isn't in the limbo region
                // between _poll_all() and wait(), where the condition variable
                // signal would be lost

                std::lock_guard<std::timed_mutex>(*data_protection.poller_mutex);
            }
            data_protection.poller_cv->notify_all();
        }
    }

  private:
    int poll(std::thread::id thread_id,
             std::unique_ptr<std::unique_lock<std::timed_mutex>>& lock) override
    {
        std::vector<std::pair<std::shared_ptr<typename Callback::CallbackType>,
                              std::shared_ptr<const Data>>>
            data_callbacks;
        int poll_items_count = 0;

        {
            std::shared_lock<std::shared_timed_mutex> sub_lock(subscription_mutex_);

            auto queue_it = data_.find(thread_id);
            if (queue_it == data_.end())
                return 0; // no subscriptions

            std::unique_lock<std::mutex> data_lock(
                *(data_protection_.find(thread_id)->second.data_mutex));

            // loop over all Groups stored in this DataQueue
            for (auto data_it = queue_it->second.cbegin(), end = queue_it->second.cend();
                 data_it != end; ++data_it)
            {
                const Group& group = data_it->first;
                auto group_range = subscription_groups_.equal_range(group);
                // For a given Group, loop over all subscriptions to this Group
                for (auto group_it = group_range.first; group_it != group_range.second; ++group_it)
                {
                    if (group_it->second->first != thread_id)
                        continue;

                    // store the callback function and datum for all the elements queued
                    for (auto& datum : data_it->second)
                    {
                        ++poll_items_count;
                        // we have data, no need to keep this lock any longer
                        if (lock)
                            lock.reset();
                        data_callbacks.push_back(
                            std::make_pair(group_it->second->second.callback, datum));
                    }
                }
                queue_it->second.clear(group);
            }
        }

        // now that we're no longer blocking the subscription or data mutex, actually run the callbacks
        for (const auto& callback_datum_pair : data_callbacks)
            (*callback_datum_pair.first)(std::move(callback_datum_pair.second));

        return poll_items_count;
    }

    void unsubscribe_all_groups(std::thread::id thread_id) override
    {
        {
            std::lock_guard<std::shared_timed_mutex> lock(subscription_mutex_);

            for (auto it = subscription_groups_.begin(); it != subscription_groups_.end();)
            {
                auto sub_thread_id = it->second->first;

                if (sub_thread_id == thread_id)
                {
                    subscription_callbacks_.erase(it->second);
                    it = subscription_groups_.erase(it);
                }
                else
                {
                    ++it;
                }
            }

            data_.erase(thread_id);
        }
    }

  private:
    struct Callback
    {
        using CallbackType = std::function<void(std::shared_ptr<const Data>)>;
        Callback(const Group& g, const std::function<void(std::shared_ptr<const Data>)>& c)
            : group(g), callback(new CallbackType(c))
        {
        }
        Group group;
        std::shared_ptr<CallbackType> callback;
    };

    class DataQueue
    {
      private:
        std::unordered_map<Group, std::vector<std::shared_ptr<const Data>>> data_;

      public:
        void create(const Group& g)
        {
            auto it = data_.find(g);
            if (it == data_.end())
                data_.insert(std::make_pair(g, std::vector<std::shared_ptr<const Data>>()));
        }
        void remove(const Group& g) { data_.erase(g); }

        void insert(const Group& g, std::shared_ptr<const Data> datum)
        {
            data_.find(g)->second.push_back(datum);
        }
        void clear(const Group& g) { data_.find(g)->second.clear(); }
        bool empty() { return data_.empty(); }
        typename decltype(data_)::const_iterator cbegin() { return data_.begin(); }
        typename decltype(data_)::const_iterator cend() { return data_.end(); }
    };

    // subscriptions for a given thread
    static std::unordered_multimap<std::thread::id, Callback> subscription_callbacks_;
    // threads that are subscribed to a given group
    static std::unordered_multimap<Group,
                                   typename decltype(subscription_callbacks_)::const_iterator>
        subscription_groups_;
    // condition variable to use for data
    static std::unordered_map<std::thread::id, detail::DataProtection> data_protection_;

    static std::shared_timed_mutex
        subscription_mutex_; // protects subscription_callbacks, subscription_groups, data_protection, and the overarching data_ map (but not the DataQueues within it, which are protected by the mutexes stored in data_protection_))

    // data for a given thread
    static std::unordered_map<std::thread::id, DataQueue> data_;
};

template <typename Data>
std::unordered_multimap<std::thread::id, typename SubscriptionStore<Data>::Callback>
    SubscriptionStore<Data>::subscription_callbacks_;
template <typename Data>
std::unordered_map<std::thread::id, typename SubscriptionStore<Data>::DataQueue>
    SubscriptionStore<Data>::data_;
template <typename Data>
std::unordered_multimap<goby::middleware::Group,
                        typename decltype(
                            SubscriptionStore<Data>::subscription_callbacks_)::const_iterator>
    SubscriptionStore<Data>::subscription_groups_;
template <typename Data>
std::unordered_map<std::thread::id, detail::DataProtection>
    SubscriptionStore<Data>::data_protection_;

template <typename Data> std::shared_timed_mutex SubscriptionStore<Data>::subscription_mutex_;

} // namespace detail
} // namespace middleware
} // namespace goby

#endif
