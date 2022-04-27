// Copyright 2020-2021:
//   GobySoft, LLC (2013-)
//   Community contributors (see AUTHORS file)
// File authors:
//   Toby Schneider <toby@gobysoft.org>
//   Shawn Dooley <shawn@shawndooley.net>
// Modified by: 
//   Lily Muir <lmuir414@gmail.com>
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

#ifndef GOBY_MIDDLEWARE_IO_CAN_H
#define GOBY_MIDDLEWARE_IO_CAN_H

#include <errno.h>         // for errno
#include <linux/can.h>     // for can_frame, socka...
#include <linux/can/raw.h> // for CAN_RAW_FILTER
#include <memory>          // for make_shared, sha...
#include <net/if.h>        // for ifreq, ifr_ifindex
#include <stdint.h>        // for uint32_t, uint8_t
#include <string.h>        // for strcpy, strerror
#include <string>          // for string, operator+
#include <sys/ioctl.h>     // for ioctl, SIOCGIFINDEX
#include <sys/socket.h>    // for bind, setsockopt
#include <tuple>           // for make_tuple, tuple
#include <vector>          // for vector
#include <deque>           // for fastpacket outbox queue

#include <boost/asio/buffer.hpp>                  // for buffer
#include <boost/asio/posix/stream_descriptor.hpp> // for stream_descriptor
#include <boost/asio/read.hpp>                    // for async_read
#include <boost/system/error_code.hpp>           // for error_code
#include <boost/bind.hpp>                         // for bind
#include <boost/core/ref.hpp>                     // for ref

#include "goby/exception.h"                         // for Exception
#include "goby/middleware/io/detail/io_interface.h" // for PubSubLayer, IOT...
#include "goby/middleware/protobuf/can_config.pb.h" // for CanConfig, CanCo...
#include "goby/middleware/protobuf/io.pb.h"         // for IOData
namespace goby
{
namespace middleware
{
class Group;
}
} // namespace goby

namespace goby
{
namespace middleware
{
namespace io
{
inline std::uint32_t make_extended_format_can_id(std::uint32_t pgn, std::uint8_t priority,
                                                 std::uint8_t source = 0)
{
    return (pgn & 0x1FFFF) << 8 | (priority & 0x7) << 26 | CAN_EFF_FLAG | (source & 0xFF);
}

// tuple of pgn, priority, source
namespace can_id
{
constexpr int pgn_index{0};
constexpr int priority_index{1};
constexpr int source_index{2};
} // namespace can_id

inline std::tuple<std::uint32_t, std::uint8_t, std::uint8_t>
parse_extended_format_can_id(std::uint32_t can_id)
{
    return std::make_tuple((can_id >> 8) & 0x1FFFF, (can_id >> 26) & 0x7, can_id & 0xFF);
}

template <const goby::middleware::Group& line_in_group,
          const goby::middleware::Group& line_out_group,
          // by default publish all incoming traffic to interprocess for logging
          PubSubLayer publish_layer = PubSubLayer::INTERPROCESS,
          // but only subscribe on interthread for outgoing traffic
          PubSubLayer subscribe_layer = PubSubLayer::INTERTHREAD,
          template <class> class ThreadType = goby::middleware::SimpleThread>
class CanThread : public detail::IOThread<line_in_group, line_out_group, publish_layer,
                                          subscribe_layer, goby::middleware::protobuf::CanConfig,
                                          boost::asio::posix::stream_descriptor, ThreadType>
{
    using Base = detail::IOThread<line_in_group, line_out_group, publish_layer, subscribe_layer,
                                  goby::middleware::protobuf::CanConfig,
                                  boost::asio::posix::stream_descriptor, ThreadType>;

  public:
    /// \brief Constructs the thread.
    /// \param config A reference to the Protocol Buffers config read by the main application at launch
    /// \param index Thread index for multiple instances in a given application (-1 indicates a single instance)
    CanThread(const goby::middleware::protobuf::CanConfig& config, int index = -1)
        : Base(config, index, std::string("can: ") + config.interface())
    {
        auto ready = ThreadState::SUBSCRIPTIONS_COMPLETE;
        this->interthread().template publish<line_in_group>(ready);
    }

    ~CanThread() override {}

  private:
    void async_read() override;
    void async_write(std::shared_ptr<const goby::middleware::protobuf::IOData> io_msg) override;

    void open_socket() override;

    void data_rec(struct can_frame& receive_frame_, boost::asio::posix::stream_descriptor& stream);

  private:
    typedef std::deque<goby::middleware::protobuf::IOData> Outbox;

  private:
    struct can_frame receive_frame_;

    Outbox write_outbox_; // need to ensure the fastpackets are sent back to back
};
} // namespace io
} // namespace middleware
} // namespace goby

template <const goby::middleware::Group& line_in_group,
          const goby::middleware::Group& line_out_group,
          goby::middleware::io::PubSubLayer publish_layer,
          goby::middleware::io::PubSubLayer subscribe_layer, template <class> class ThreadType>
void goby::middleware::io::CanThread<line_in_group, line_out_group, publish_layer, subscribe_layer,
                                     ThreadType>::open_socket()
{
    int can_socket;

    struct sockaddr_can addr_
    {
    };
    struct can_frame receive_frame_;
    struct ifreq ifr_;
    can_socket = socket(PF_CAN, SOCK_RAW, CAN_RAW);

    std::vector<struct can_filter> filters;

    for (auto x : this->cfg().filter())
    {
        auto id = x.can_id();
        auto mask = x.has_can_mask_custom() ? x.can_mask_custom() : x.can_mask();

        filters.push_back({id, mask});
    }

    for (std::uint32_t x : this->cfg().pgn_filter())
    {
        constexpr std::uint32_t one_byte = 8; // bits
        auto id = x << one_byte;
        constexpr auto mask = protobuf::CanConfig::CanFilter::PGNOnly; // PGN mask
        filters.push_back({id, mask});
    }

    if (filters.size())
    {
        setsockopt(can_socket, SOL_CAN_RAW, CAN_RAW_FILTER, filters.data(),
                   sizeof(can_filter) * filters.size());
    }
    std::strcpy(ifr_.ifr_name, this->cfg().interface().c_str());

    ioctl(can_socket, SIOCGIFINDEX, &ifr_);

    addr_.can_family = AF_CAN;
    addr_.can_ifindex = ifr_.ifr_ifindex;
    if (bind(can_socket, (struct sockaddr*)&addr_, sizeof(addr_)) < 0)
        throw(goby::Exception(std::string("Error in socket bind to interface ") +
                              this->cfg().interface() + ": " + std::strerror(errno)));

    this->mutable_socket().assign(can_socket);

    this->interthread().template subscribe<line_out_group, can_frame>(
        [this](const can_frame& frame) {
            auto io_msg = std::make_shared<goby::middleware::protobuf::IOData>();
            std::string& bytes = *io_msg->mutable_data();

            const int frame_size = sizeof(can_frame);

            for (int i = 0; i < frame_size; ++i)
            { bytes += *(reinterpret_cast<const char*>(&frame) + i); } 

            // check current write outbox to see if message is already queued
            bool in_write_buffer = false; 
            for (int i = 0; i < write_outbox_.size(); ++i){
                if (write_outbox_[i].data() == io_msg->data()){
                    // this message is in the write buffer already, break loop
                    in_write_buffer = true;
                    break;
                }
            }
            if (!in_write_buffer){
                write_outbox_.push_back(*io_msg);

                if ( write_outbox_.size() == 1 ) {
                    this->write(io_msg);
                }
                else{
                    goby::glog.is_debug1() && goby::glog << "Outstanding write, added message to outbox buffer" << std::endl;
                }
            }
            else{
                goby::glog.is_debug1() && goby::glog << "Message already Queued in buffer to be written" << std::endl;
            }
        });
}

template <const goby::middleware::Group& line_in_group,
          const goby::middleware::Group& line_out_group,
          goby::middleware::io::PubSubLayer publish_layer,
          goby::middleware::io::PubSubLayer subscribe_layer, template <class> class ThreadType>
void goby::middleware::io::CanThread<line_in_group, line_out_group, publish_layer, subscribe_layer,
                                     ThreadType>::async_read()
{
    boost::asio::async_read(this->mutable_socket(),
                            boost::asio::buffer(&receive_frame_, sizeof(receive_frame_)),
                            boost::bind(&CanThread::data_rec, this, boost::ref(receive_frame_),
                                        boost::ref(this->mutable_socket())));
}

template <const goby::middleware::Group& line_in_group,
          const goby::middleware::Group& line_out_group,
          goby::middleware::io::PubSubLayer publish_layer,
          goby::middleware::io::PubSubLayer subscribe_layer, template <class> class ThreadType>
void goby::middleware::io::CanThread<line_in_group, line_out_group, publish_layer, subscribe_layer,
                                     ThreadType>::data_rec(struct can_frame& receive_frame_,
                                                           boost::asio::posix::stream_descriptor&
                                                               stream)
{
    //  Within a process raw can frames are probably what we are looking for.
    this->interthread().template publish<line_in_group>(receive_frame_);

    std::string bytes;
    const int frame_size = sizeof(can_frame);

    for (int i = 0; i < frame_size; ++i)
    { bytes += *(reinterpret_cast<char*>(&receive_frame_) + i); }

    this->handle_read_success(bytes.size(), bytes);

    boost::asio::async_read(
        stream, boost::asio::buffer(&receive_frame_, sizeof(receive_frame_)),
        boost::bind(&CanThread::data_rec, this, boost::ref(receive_frame_), boost::ref(stream)));
}

template <const goby::middleware::Group& line_in_group,
          const goby::middleware::Group& line_out_group,
          goby::middleware::io::PubSubLayer publish_layer,
          goby::middleware::io::PubSubLayer subscribe_layer, template <class> class ThreadType>
void goby::middleware::io::CanThread<line_in_group, line_out_group, publish_layer, subscribe_layer,
                                     ThreadType>::async_write(std::shared_ptr<const goby::middleware::protobuf::IOData> io_msg)
{
    glog.is_debug2() && glog << "Size of buffer? " << write_outbox_.size() << std::endl;
    boost::asio::async_write(
        this->mutable_socket(), boost::asio::buffer(io_msg->data()),
        // capture io_msg in callback to ensure write buffer exists until async_write is done
        [this, io_msg](const boost::system::error_code& ec, std::size_t bytes_transferred) {
            if (!ec && bytes_transferred > 0)
            {
                this->handle_write_success(bytes_transferred);
            }
            else
            {
                check_write_error(ec);
            }
            write_outbox_.pop_front();
            glog.is_debug2() && glog << "Check Size of buffer? " << write_outbox_.size() << std::endl;
            if ( !write_outbox_.empty() ) {
                // send next message
                auto io_msg = std::make_shared<goby::middleware::protobuf::IOData>(write_outbox_[0]);
                this->write(io_msg);
            }
        }
    );
}

#endif
