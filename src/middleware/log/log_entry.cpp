// Copyright 2017-2021:
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

#include "log_entry.h"

#include <algorithm>                             // for copy, max
#include <boost/iterator/iterator_facade.hpp>    // for operator!=, iter...
#include <boost/multi_index/sequenced_index.hpp> // for operator==

#include "goby/middleware/marshalling/interface.h" // for MarshallingScheme
#include "goby/time/convert.h"
#include "goby/time/types.h"
#include "goby/util/debug_logger/flex_ostream.h"    // for operator<<, Flex...
#include "goby/util/debug_logger/flex_ostreambuf.h" // for DEBUG1, WARN

enum VersionFeatures
{
    VERSION_ADD_TYPE_GROUP_HOOKS = 2,
    VERSION_ADD_VERSION_NUMBER = 2,
    VERSION_ADD_SCHEME_TO_GROUP_TYPE_MAPPING = 2,
    VERSION_ADD_TIMESTAMP = 3
};

using goby::middleware::log::LogEntry;

std::map<int, boost::bimap<std::string, goby::middleware::log::uint<LogEntry::group_bytes_>::type>>
    LogEntry::groups_;
std::map<int, boost::bimap<std::string, goby::middleware::log::uint<LogEntry::type_bytes_>::type>>
    LogEntry::types_;

std::map<int, std::function<void(const std::string& type)>> LogEntry::new_type_hook;
std::map<int, std::function<void(const goby::middleware::Group& group)>> LogEntry::new_group_hook;

std::map<goby::middleware::log::LogFilter,
         std::function<void(const std::vector<unsigned char>& data)>>
    LogEntry::filter_hook;

goby::middleware::log::uint<LogEntry::group_bytes_>::type LogEntry::group_index_(1);
goby::middleware::log::uint<LogEntry::type_bytes_>::type LogEntry::type_index_(1);

goby::middleware::log::uint<LogEntry::version_bytes_>::type
    LogEntry::version_(LogEntry::invalid_version);

int LogEntry::current_version_(LogEntry::compiled_current_version);

void LogEntry::parse_version(std::istream* s)
{
    version_ = read_one<uint<version_bytes_>::type>(s);

    // Original file format didn't have a version, so "GB" would be the version bytes
    // (first two characters of the magic word)
    if (version_ == string_to_netint<decltype(version_)>(magic_))
    {
        version_ = 1;
        // rewind
        s->seekg(s->tellg() - std::streamoff(version_bytes_));
    }
    else if (version_ > current_version_)
    {
        glog.is_warn() && glog << "Version 0x" << std::hex << version_
                               << " is invalid. Will try to read file using current version ("
                               << std::dec << current_version_ << ")" << std::endl;
        version_ = current_version_;
    }

    glog.is_verbose() && glog << "File version is " << version_ << std::endl;
}

void LogEntry::parse(std::istream* s)
{
    using namespace goby::util::logger;
    using goby::glog;

    if (version_ == invalid_version)
        parse_version(s);

    auto old_except_mask = s->exceptions();
    s->exceptions(std::ios::failbit | std::ios::badbit | std::ios::eofbit);

    int legacy_scheme = goby::middleware::MarshallingScheme::NULL_SCHEME;

    uint<scheme_bytes_>::type scheme(0);

    bool filter_matched = false;
    do
    {
        char next_char = s->peek();
        if (next_char != magic_[0])
        {
            glog.is(WARN) && glog << "Next byte [0x" << std::hex
                                  << (static_cast<int>(next_char) & 0xFF) << std::dec
                                  << "] is not the start of the expected magic word [" << magic_
                                  << "]. Seeking until next magic word." << std::endl;
        }

        std::string magic_read(magic_.size(), '\0');
        int discarded = 0;

        for (;;)
        {
            s->read(&magic_read[0], magic_.size());
            if (magic_read == magic_)
            {
                break;
            }
            else
            {
                ++discarded;
                // rewind to read the next byte
                s->seekg(s->tellg() - std::streamoff(magic_.size() - 1));
            }
        }

        if (discarded != 0)
            glog.is(WARN) && glog << "Found next magic word after skipping " << discarded
                                  << " bytes" << std::endl;

        boost::crc_32_type crc;
        crc.process_bytes(&magic_read[0], magic_.size());

        auto size(read_one<uint<size_bytes_>::type>(s, &crc));
        decltype(size) fixed_field_size = scheme_bytes_ + group_bytes_ + type_bytes_ + crc_bytes_;
        if (version_ >= VERSION_ADD_TIMESTAMP)
            fixed_field_size += timestamp_bytes_;

        if (size < fixed_field_size)
            throw(log::LogException("Invalid size read: " + std::to_string(size) +
                                    " as message must be at least " +
                                    std::to_string(fixed_field_size) + " bytes long"));

        auto data_size = size - fixed_field_size;
        glog.is(DEBUG2) && glog << "Reading entry of " << size << " bytes (" << data_size
                                << " bytes data)" << std::endl;

        scheme = read_one<uint<scheme_bytes_>::type>(s, &crc);
        auto group_index(read_one<uint<group_bytes_>::type>(s, &crc));
        auto type_index(read_one<uint<type_bytes_>::type>(s, &crc));
        if (version_ >= VERSION_ADD_TIMESTAMP)
        {
            auto timestamp(read_one<uint<timestamp_bytes_>::type>(s, &crc));
            glog.is(DEBUG2) && glog << "Timestamp: " << timestamp << " microseconds" << std::endl;
            timestamp_ = goby::time::convert<decltype(timestamp_)>(
                timestamp * boost::units::si::micro * boost::units::si::seconds);
        }

        auto data_start_pos = s->tellg();
        try
        {
            data_.resize(data_size);
            s->read(reinterpret_cast<char*>(&data_[0]), data_size);

            crc.process_bytes(&data_[0], data_.size());

            auto calculated_crc = crc.checksum();
            auto given_crc(read_one<uint<crc_bytes_>::type>(s));

            if (calculated_crc != given_crc)
            {
                // return to where we started reading data as the size might have been corrupt
                s->seekg(data_start_pos);
                data_.clear();
                throw(
                    log::LogException("Invalid CRC on packet: given: " + std::to_string(given_crc) +
                                      ", calculated: " + std::to_string(calculated_crc)));
            }
        }
        catch (std::ios_base::failure& e)
        {
            // clear EOF, etc.
            s->clear();
            // return to where data reading starting in case size was corrupted
            s->seekg(data_start_pos);
            throw(log::LogException("Failed to read " + std::to_string(size) +
                                    " bytes of data; seeking back to start of data read in hopes "
                                    "of finding valid next message."));
        }

        if (scheme == scheme_group_index_)
        {
            if (version_ < VERSION_ADD_SCHEME_TO_GROUP_TYPE_MAPPING)
            {
                std::string group(data_.begin(), data_.end());

                // The first type of .goby files that used a single mapping of type/group
                // string for all schemes. This worked fine unless the two schemes are in use that had a common type name.
                glog.is(DEBUG1) && glog << "Mapping group [" << group
                                        << "] to index: " << group_index << std::endl;

                groups_[legacy_scheme].left.insert({group, group_index});
            }
            else
            {
                std::string group_scheme_str(data_.begin(), data_.begin() + scheme_bytes_);
                auto group_scheme = string_to_netint<uint<scheme_bytes_>::type>(group_scheme_str);

                std::string group(data_.begin() + scheme_bytes_, data_.end());
                glog.is(DEBUG1) && glog << "For scheme [" << group_scheme << "], mapping group ["
                                        << group << "] to index: " << group_index << std::endl;
                groups_[group_scheme].left.insert({group, group_index});

                if (new_group_hook[group_scheme])
                    new_group_hook[group_scheme](goby::middleware::DynamicGroup(group));
            }
            data_.clear();
        }
        else if (scheme == scheme_type_index_)
        {
            if (version_ < VERSION_ADD_SCHEME_TO_GROUP_TYPE_MAPPING)
            {
                std::string type(data_.begin(), data_.end());
                glog.is(DEBUG1) && glog << "Mapping type [" << type << "] to index: " << type_index
                                        << std::endl;
                types_[legacy_scheme].left.insert({type, type_index});
            }
            else
            {
                std::string type_scheme_str(data_.begin(), data_.begin() + scheme_bytes_);
                auto type_scheme = string_to_netint<uint<scheme_bytes_>::type>(type_scheme_str);

                std::string type(data_.begin() + scheme_bytes_, data_.end());
                glog.is(DEBUG1) && glog << "For scheme [" << type_scheme << "], mapping type ["
                                        << type << "] to index: " << type_index << std::endl;
                types_[type_scheme].left.insert({type, type_index});

                if (new_type_hook[type_scheme])
                    new_type_hook[type_scheme](type);
            }

            data_.clear();
        }
        else
        {
            scheme_ = scheme;

            std::string type = "_unknown" + std::to_string(type_index) + "_";
            auto type_it = types_[scheme].right.find(type_index),
                 type_end_it = types_[scheme].right.end();

            if (version_ < VERSION_ADD_SCHEME_TO_GROUP_TYPE_MAPPING)
            {
                type_it = types_[legacy_scheme].right.find(type_index);
                type_end_it = types_[legacy_scheme].right.end();
            }

            if (type_it != type_end_it)
                type = type_it->second;
            else
                glog.is(WARN) && glog << "No type entry in file for type index: " << type_index
                                      << std::endl;

            type_ = type;

            std::string group = "_unknown" + std::to_string(group_index) + "_";
            auto group_it = groups_[scheme].right.find(group_index),
                 group_end_it = groups_[scheme].right.end();

            if (version_ < VERSION_ADD_SCHEME_TO_GROUP_TYPE_MAPPING)
            {
                group_it = groups_[legacy_scheme].right.find(group_index);
                group_end_it = groups_[legacy_scheme].right.end();
            }

            if (group_it != group_end_it)
                group = group_it->second;
            else
                glog.is(WARN) && glog << "No group entry in file for group index: " << group_index
                                      << std::endl;

            group_ = goby::middleware::DynamicGroup(group);

            LogFilter filt{scheme_, group, type_};
            if (filter_hook.count(filt))
            {
                filter_matched = true;
                filter_hook[filt](data_);
            }
            else
            {
                filter_matched = false;
            }
        }
    } while (scheme == scheme_group_index_ || scheme == scheme_type_index_ || filter_matched);

    s->exceptions(old_except_mask);
}

void LogEntry::serialize(std::ostream* s) const
{
    auto old_except_mask = s->exceptions();
    s->exceptions(std::ios::failbit | std::ios::badbit | std::ios::eofbit);

    // write version
    if (version_ == invalid_version)
    {
        version_ = current_version_;

        // version tagging started at version 2
        if (current_version_ >= VERSION_ADD_VERSION_NUMBER)
        {
            std::string version_str(netint_to_string(version_));
            s->write(version_str.data(), version_str.size());
        }
    }

    std::string group(group_);

    int legacy_scheme = goby::middleware::MarshallingScheme::NULL_SCHEME;
    auto scheme_mapping =
        (version_ >= VERSION_ADD_SCHEME_TO_GROUP_TYPE_MAPPING) ? scheme_ : legacy_scheme;

    // insert indexing entry if the first time we saw this group
    if (groups_[scheme_mapping].left.count(group) == 0)
    {
        auto index = group_index_++;
        groups_[scheme_mapping].left.insert({group, index});

        std::string scheme_str(netint_to_string(scheme_));
        std::string scheme_plus_group =
            (version_ >= VERSION_ADD_SCHEME_TO_GROUP_TYPE_MAPPING) ? scheme_str + group : group;
        _serialize(s, scheme_group_index_, index, 0, scheme_plus_group.data(),
                   scheme_plus_group.size());

        if (version_ >= VERSION_ADD_TYPE_GROUP_HOOKS && new_group_hook[scheme_mapping])
            new_group_hook[scheme_mapping](group_);
    }
    if (types_[scheme_mapping].left.count(type_) == 0)
    {
        auto index = type_index_++;
        types_[scheme_mapping].left.insert({type_, index});

        std::string scheme_str(netint_to_string(scheme_));
        std::string scheme_plus_type =
            (version_ >= VERSION_ADD_SCHEME_TO_GROUP_TYPE_MAPPING) ? scheme_str + type_ : type_;
        _serialize(s, scheme_type_index_, 0, index, scheme_plus_type.data(),
                   scheme_plus_type.size());

        if (version_ >= VERSION_ADD_TYPE_GROUP_HOOKS && new_type_hook[scheme_mapping])
            new_type_hook[scheme_mapping](type_);
    }

    auto group_index = groups_[scheme_mapping].left.at(group);
    auto type_index = types_[scheme_mapping].left.at(type_);

    // insert actual data
    _serialize(s, scheme_, group_index, type_index, reinterpret_cast<const char*>(&data_[0]),
               data_.size());

    s->exceptions(old_except_mask);
}

void LogEntry::_serialize(std::ostream* s, uint<scheme_bytes_>::type scheme,
                          uint<group_bytes_>::type group_index, uint<type_bytes_>::type type_index,
                          const char* data, int data_size) const
{
    std::string group_str(netint_to_string(group_index));
    std::string type_str(netint_to_string(type_index));
    std::string scheme_str(netint_to_string(scheme));

    uint<size_bytes_>::type size =
        scheme_bytes_ + group_bytes_ + type_bytes_ + data_size + crc_bytes_;

    if (version_ >= VERSION_ADD_TIMESTAMP)
        size += timestamp_bytes_;

    std::string size_str(netint_to_string(size));

    auto header = magic_ + size_str + scheme_str + group_str + type_str;
    if (version_ >= VERSION_ADD_TIMESTAMP)
    {
        std::uint64_t timestamp = goby::time::convert<goby::time::MicroTime>(timestamp_).value();
        std::string timestamp_str(netint_to_string(timestamp));
        header += timestamp_str;
    }

    s->write(header.data(), header.size());
    s->write(data, data_size);

    boost::crc_32_type crc;
    crc.process_bytes(header.data(), header.size());
    crc.process_bytes(data, data_size);

    uint<crc_bytes_>::type cs(crc.checksum());
    std::string cs_str(netint_to_string(cs));
    s->write(cs_str.data(), cs_str.size());
}
