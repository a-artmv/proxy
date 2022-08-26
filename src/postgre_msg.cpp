#include "postgre_msg.h"

#include <sstream>
#include <stdexcept>
#include <vector>

#define PG_PROTOCOL(m,n)	(((m) << 16) | (n))

namespace postgre_msg_nms {

using std::ostringstream;
using std::runtime_error;
using std::vector;

namespace message_state {

    const int out_of_sync = -1;
    const int waiting_for_type = 0;
    const int waiting_for_size1 = 1;
    //const int waiting_for_size2 = 2;
    //const int waiting_for_size3 = 3;
    const int waiting_for_size4 = 4;
    const int waiting_for_data = 5;

}

using namespace message_state;

namespace message_type {

    const int typeless_message = 0;
    const int typed_message = 1;

}

using namespace message_type;

namespace message_id {

    const int startup_message = PG_PROTOCOL(3,0);
    const int ssl_request = PG_PROTOCOL(1234,5679);
    const int gss_enc_request = PG_PROTOCOL(1234,5680);
    const int cancel_request = PG_PROTOCOL(1234,5678);

    const char bind_id = 'B';
    const char close_id = 'C';
    const char copy_data_id = 'd';
    const char copy_done_id = 'c';
    const char copy_fail_id = 'f';
    const char describe_id = 'D';
    const char execute_id = 'E';
    const char flush_id = 'H';
    const char function_call_id = 'F';
    const char parse_id = 'P';
    const char query_id = 'Q';
    const char sync_id = 'S';
    const char terminate_id = 'X';
    const char passw_id = 'p';

}

using namespace message_id;

const int max_data_size = 1024 * 1024;  // bytes

protocol_message_t::protocol_message_t() : cur_size_(0), state_(waiting_for_type) {}

void protocol_message_t::throw_error(ofstream &log, const ExceptF &except_f) {
    data_ = queue<page_wrapper_t>();
    cur_size_ = 0;
    state_ = out_of_sync;
    except_f([&](){ log << " ! logger error !\n"; });
    throw runtime_error("error while processing SQL query");
}

void protocol_message_t::process(page_wrapper_t &&wpage, const string &preamble
                                 , ofstream &log, const ExceptF &except_f) {
    const auto skip_data = [&](){
        wpage.adjust_pos(size_ - cur_size_);
        data_ = queue<page_wrapper_t>();
        size_ = 0;
    };
    const auto too_big = [&](){
        if(max_data_size < cur_size_){
            except_f([&](){
                log << " ! Query was too big: ";
                log << std::to_string(size_);
                log << " bytes !";
            });
            skip_data();
            return true;
        }
        return false;
    };
    const auto get_byte = [&](){
        page_wrapper_t *pwp = data_.size() ? &data_.front() : &wpage;
        if(pwp->size() == 0){ throw_error(log, except_f); }
        int8_t val = *(pwp->data());
        pwp->adjust_pos(1);
        size_ -= 1;
        if(size_ < 0){ throw_error(log, except_f); }
        if(pwp->size() == 0 && data_.size()){
            data_.pop();
        }
        return val;
    };
    const auto get_int2 = [&](){
        int8_t b[2];
        b[1] = get_byte();
        b[0] = get_byte();
        return *reinterpret_cast<uint16_t*>(&b[0]);
    };
    const auto get_int4 = [&](){
        int8_t b[4];
        b[3] = get_byte();
        b[2] = get_byte();
        b[1] = get_byte();
        b[0] = get_byte();
        return *reinterpret_cast<uint32_t*>(&b[0]);
    };
    const auto c_str = [&](){
        char c = get_byte();
        while(c){
            except_f([&](){ log << c; });
            c = get_byte();
        }
    };
    const auto params_pack = [&](){
        auto fmt = get_int2();
        vector<bool> formats;
        if(fmt){
            except_f([&](){
                formats.reserve(fmt);
                log << " fmt_codes=";
            });
            while(fmt--){
                auto v = get_int2();
                formats.push_back(v);
                except_f([&](){
                    log << std::to_string(v);
                    if(fmt){ log << ','; }
                });
            }
        }
        auto prm = get_int2();
        if(prm){
            except_f([&](){ log << " params="; });
            bool default_fmt = false;
            if(formats.size() == 1){
                default_fmt = formats[0];
            }
            while(prm--){
                auto v = static_cast<int32_t>(get_int4());
                if(v == -1){
                    except_f([&](){ log << "NULL"; });
                } else if(v == 0){
                    except_f([&](){ log << "EMPTY"; });
                } else{
                    bool binary = default_fmt;
                    if(prm < formats.size()){
                        binary = formats[prm];
                    }
                    while(v--){
                        auto b = get_byte();
                        if(binary){
                            except_f([&](){
                                ostringstream ss;
                                ss << std::hex << static_cast<unsigned>(b);
                                log << ss.str();
                            });
                        } else{
                            except_f([&](){ log << b; });
                        }
                    }
                }
                if(prm){ except_f([&](){ log << ','; }); }
            }
        }
    };
    except_f([&](){ log << preamble; });
    if(type_ == typeless_message){
        auto i = get_int4();
        switch(i){
        case startup_message:
        {
            except_f([&](){ log << "[Startup Message]"; });
            if(!too_big()){
                while(1 < size_){
                    except_f([&](){ log << ' '; });
                    c_str();
                    except_f([&](){ log << '='; });
                    c_str();
                }
                if(get_byte()){
                    throw_error(log, except_f);
                }
            }
            break;
        }
        case ssl_request:
        {
            except_f([&](){ log << "[SSL request]"; });
            break;
        }
        case gss_enc_request:
        {
            except_f([&](){ log << "[GSS Encryption request]"; });
            break;
        }
        case cancel_request:
        {
            except_f([&](){ log << "[Cancel request]"; });
            auto pid = get_int4();
            auto key = get_int4();
            except_f([&](){
                log << " PID=" << std::to_string(pid);
                log << " key=" << std::to_string(key);
            });
            break;
        }
        default:
            throw_error(log, except_f);
        }
    } else{
        switch(type_byte_){
        case bind_id:
        {
            except_f([&](){ log << "[Bind command]"; });
            if(!too_big()){
                except_f([&](){ log << " dest_portal="; });
                c_str();
                except_f([&](){ log << " prep_statement="; });
                c_str();
                params_pack();
                auto rsl = get_int2();
                if(rsl){
                    except_f([&](){
                        log << " res_fmt_codes=";
                    });
                    while(rsl--){
                        auto v = get_int2();
                        except_f([&](){
                            log << std::to_string(v);
                            if(rsl){ log << ','; }
                        });
                    }
                }
            }
            break;
        }
        case close_id:
        {
            except_f([&](){ log << "[Close command]"; });
            if(!too_big()){
                switch(get_byte()){
                case 'S':
                    except_f([&](){ log << " prep_statement="; });
                    break;
                case 'P':
                    except_f([&](){ log << " portal="; });
                    break;
                default:
                    throw_error(log, except_f);
                }
                c_str();
            }
            break;
        }
        case copy_fail_id:
        {
            except_f([&](){ log << "[COPY failure]"; });
            if(!too_big()){
                except_f([&](){ log << " error_mgs="; });
                c_str();
            }
            break;
        }
        case describe_id:
        {
            except_f([&](){ log << "[Describe command]"; });
            if(!too_big()){
                switch(get_byte()){
                case 'S':
                    except_f([&](){ log << " prep_statement="; });
                    break;
                case 'P':
                    except_f([&](){ log << " portal="; });
                    break;
                default:
                    throw_error(log, except_f);
                }
                c_str();
            }
            break;
        }
        case execute_id:
        {
            except_f([&](){ log << "[Execute command]"; });
            if(!too_big()){
                except_f([&](){ log << " portal="; });
                c_str();
                auto rows = get_int4();
                except_f([&](){ log << " max_rows=" << std::to_string(rows); });
            }
            break;
        }
        case function_call_id:
        {
            except_f([&](){ log << "[function call]"; });
            if(!too_big()){
                auto fid = get_int4();
                except_f([&](){ log << " function_id=" << std::to_string(fid); });
                params_pack();
                auto r = get_int2();
                except_f([&](){ log << " result_fmt=" << std::to_string(r); });
            }
            break;
        }
        case copy_data_id:
        {
            except_f([&](){
                log << "[COPY data] " << std::to_string(size_) <<" bytes";
            });
            skip_data();
            break;
        }
        case parse_id:
        {
            except_f([&](){ log << "[Parse command]"; });
            if(!too_big()){
                except_f([&](){ log << " prep_statement="; });
                c_str();
                except_f([&](){ log << " query="; });
                c_str();
                auto prm = get_int2();
                if(prm){
                    except_f([&](){ log << " param_types="; });
                    while(prm--){
                        auto id = get_int4();
                        except_f([&](){
                            log << std::to_string(id);
                            if(prm){ log << ','; }
                        });
                    }
                }
            }
            break;
        }
        case query_id:
        {
            except_f([&](){ log << "[simple query] "; });
            if(!too_big()){
                c_str();
            }
            break;
        }
        case passw_id:
        {
            except_f([&](){
                log << "[password message | gss response | sasl response] ";
                log << std::to_string(size_) <<" bytes";
            });
            skip_data();
            break;
        }
        case copy_done_id:
        {
            except_f([&](){ log << "[COPY complete]"; });
            break;
        }
        case flush_id:
        {
            except_f([&](){ log << "[Flush command]"; });
            break;
        }
        case sync_id:
        {
            except_f([&](){ log << "[Sync command]"; });
            break;
        }
        case terminate_id:
        {
            except_f([&](){ log << "[Termination]"; });
            break;
        }
        }
    }
    except_f([&](){ log << std::endl; });
    if(size_ || data_.size()){ throw_error(log, except_f); }
    state_ = waiting_for_type;
    cur_size_ = 0;
    if(wpage.size()){
        add_data(std::move(wpage), preamble, log, except_f);
    }
}

bool protocol_message_t::check_type() {
    if(type_ == typeless_message){
        return true;
    }
    switch(type_byte_){
    case bind_id:
    case close_id:
    case copy_fail_id:
    case describe_id:
    case execute_id:
    case function_call_id:
    case copy_data_id:
    case parse_id:
    case query_id:
    case passw_id:
        return true;
    case copy_done_id:
    case flush_id:
    case sync_id:
    case terminate_id:
        return size_ == 0;
    }
    return false;
}

void protocol_message_t::add_data(page_wrapper_t &&wpage, const string &preamble
                                  , ofstream &log, const ExceptF &except_f) {
    if(state_ == out_of_sync){
        log << preamble << "logger is out of sync. "
            << wpage.size() <<" bytes transferred\n";
        return;
    }
    if(state_ == waiting_for_type){
        type_byte_ = *reinterpret_cast<char *>(wpage.data());
        if(!type_byte_){
            type_ = typeless_message;
        } else{
            type_ = typed_message;
            wpage.adjust_pos(1);
        }
        state_ = waiting_for_size1;
        if(wpage.size() == 0){
            return;
        }
    }
    while(waiting_for_size1 <= state_ && state_ < waiting_for_size4){
        int i = state_ - waiting_for_size1;
        size_bytes_[3 - i] = *(wpage.data());
        wpage.adjust_pos(1);
        ++state_;
        if(wpage.size() == 0){
            return;
        }
    }
    if(state_ == waiting_for_size4){
        size_bytes_[0] = *(wpage.data());
        wpage.adjust_pos(1);
        size_ = *reinterpret_cast<uint32_t*>(&size_bytes_[0]);
        size_ -= 4;
        if(!check_type()){
            except_f([&](){ log << preamble; });
            throw_error(log, except_f);
        }
        state_ = waiting_for_data;
    }
    if(state_ == waiting_for_data){
        int sz = wpage.size();
        if(size_ <= cur_size_ + sz){
            process(std::move(wpage), preamble, log, except_f);
        } else if(wpage.size()){
            cur_size_ += sz;
            bool collect_data = !(type_ == typed_message && (
                                    type_byte_ == copy_data_id || type_byte_ == passw_id));
            if(collect_data && cur_size_ <= max_data_size){
                data_.emplace(std::move(wpage));
            }
        }
    }
}

}
