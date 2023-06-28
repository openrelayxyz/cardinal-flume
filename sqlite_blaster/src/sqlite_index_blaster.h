#ifndef SIARA_SQLITE_H
#define SIARA_SQLITE_H

#ifndef ARDUINO
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#endif

#include "util.h"
#include "btree_handler.h"
#include "sqlite_common.h"

#define page_resv_bytes 5

// CRTP see https://en.wikipedia.org/wiki/Curiously_recurring_template_pattern
class sqlite_index_blaster : public btree_handler<sqlite_index_blaster> {

    private:
        int U,X,M;
        // Returns type of column based on given value and length
        // See https://www.sqlite.org/fileformat.html#record_format
        uint32_t derive_col_type_or_len(int type, const void *val, int len) {
            uint32_t col_type_or_len = type;
            if (type > 11)
                col_type_or_len = len * 2 + type;
            return col_type_or_len;    
        }

        int64_t cvt_to_int64(const uint8_t *ptr, int type) {
            switch (type) {
                case SQLT_TYPE_NULL:
                case SQLT_TYPE_INT0:
                    return 0;
                case SQLT_TYPE_INT1:
                    return 1;
                case SQLT_TYPE_INT8:
                    return ptr[0];
                case SQLT_TYPE_INT16:
                    return util::read_uint16(ptr);
                case SQLT_TYPE_INT32:
                    return util::read_uint32(ptr);
                case SQLT_TYPE_INT48:
                    return util::read_uint48(ptr);
                case SQLT_TYPE_INT64:
                    return util::read_uint64(ptr);
                case SQLT_TYPE_REAL:
                    return util::read_double(ptr);
                case SQLT_TYPE_BLOB:
                case SQLT_TYPE_TEXT:
                    return 0; // TODO: do atol?
            }
            return -1; // should not reach here
        }

        // TODO: sqlite seems checking INT_MAX and INT_MIN when converting integers
        double cvt_to_dbl(const uint8_t *ptr, int type) {
            switch (type) {
                case SQLT_TYPE_REAL:
                    return util::read_double(ptr);
                case SQLT_TYPE_NULL:
                case SQLT_TYPE_INT0:
                    return 0;
                case SQLT_TYPE_INT1:
                    return 1;
                case SQLT_TYPE_INT8:
                    return ptr[0];
                case SQLT_TYPE_INT16:
                    return util::read_uint16(ptr);
                case SQLT_TYPE_INT32:
                    return util::read_uint32(ptr);
                case SQLT_TYPE_INT48:
                    return util::read_uint48(ptr);
                case SQLT_TYPE_INT64:
                    return util::read_uint64(ptr);
                case SQLT_TYPE_BLOB:
                case SQLT_TYPE_TEXT:
                    return 0; // TODO: do atol?
            }
            return -1; // should not reach here
        }

        int compare_col(const uint8_t *col1, int col_len1, int col_type1,
                            const uint8_t *col2, int col_len2, int col_type2) {
            switch (col_type1) {
                case SQLT_TYPE_BLOB:
                case SQLT_TYPE_TEXT:
                    if (col_type2 == SQLT_TYPE_TEXT || col_type2 == SQLT_TYPE_BLOB)
                        return util::compare(col1, col_len1, col2, col_len2);
                    if (col_type2 == SQLT_TYPE_NULL)
                        return 1;
                    return -1; // incompatible types
                case SQLT_TYPE_REAL: {
                    double col1_dbl = util::read_double(col1);
                    double col2_dbl = cvt_to_dbl(col2, col_type2);
                    return (col1_dbl < col2_dbl ? -1 : (col1_dbl > col2_dbl ? 1 : 0));
                    }
                case SQLT_TYPE_INT0:
                case SQLT_TYPE_INT1:
                case SQLT_TYPE_INT8:
                case SQLT_TYPE_INT16:
                case SQLT_TYPE_INT32:
                case SQLT_TYPE_INT48:
                case SQLT_TYPE_INT64: {
                    int64_t col1_int64 = cvt_to_int64(col1, col_type1);
                    int64_t col2_int64 = cvt_to_int64(col2, col_type2);
                    return (col1_int64 < col2_int64 ? -1 : (col1_int64 > col2_int64 ? 1 : 0));
                    }
                case SQLT_TYPE_NULL:
                    if (col_type2 == SQLT_TYPE_NULL)
                        return 0;
                    return -1; // NULL is less than any other type?
            }
            return -1; // should not be reached
        }

        void init() {
            U = block_size - page_resv_bytes;
            X = ((U-12)*64/255)-23;
            M = ((U-12)*32/255)-23;
            master_block = NULL;
            if (cache_size > 0) {
                if (cache->is_empty()) {
                    master_block = new uint8_t[block_size];
                    sqlite_common::fill_page0(master_block, column_count, pk_count,
                            block_size, page_resv_bytes, column_names, table_name);
                    cache->write_page(master_block, 0, block_size);
                } else {
                    master_block = new uint8_t[block_size];
                    if (cache->read_page(master_block, 0, block_size) != block_size)
                        throw 1;
                }
            }
            set_current_block_root();
        }

        uint8_t *locate_col(int which_col, uint8_t *rec, int& col_type_or_len, int& col_len, int& col_type) {
            int8_t vlen;
            int hdr_len = util::read_vint32(rec, &vlen);
            int hdr_pos = vlen;
            uint8_t *data_ptr = rec + hdr_len;
            col_len = vlen = 0;
            do {
                data_ptr += col_len;
                hdr_pos += vlen;
                if (hdr_pos >= hdr_len)
                    return NULL;
                col_type_or_len = util::read_vint32(rec + hdr_pos, &vlen);
                col_len = derive_data_len(col_type_or_len);
                col_type = derive_col_type(col_type_or_len);
            } while (which_col--);
            return data_ptr;
        }

        int compare_keys(const uint8_t *rec1, int rec1_len, const uint8_t *rec2, int rec2_len) {
            int8_t vlen;
            const uint8_t *ptr1 = rec1;
            int hdr1_len = util::read_vint32(ptr1, &vlen);
            const uint8_t *data_ptr1 = ptr1 + hdr1_len;
            ptr1 += vlen;
            const uint8_t *ptr2 = rec2;
            int hdr2_len = util::read_vint32(ptr2, &vlen);
            const uint8_t *data_ptr2 = ptr2 + hdr2_len;
            ptr2 += vlen;
            int cols_compared = 0;
            int cmp = -1;
            for (int i = 0; i < hdr1_len; i++) {
                int col_len1 = util::read_vint32(ptr1, &vlen);
                int col_type1 = col_len1;
                if (col_len1 >= 12) {
                    if (col_len1 % 2) {
                        col_len1 = (col_len1 - 12) / 2;
                        col_type1 = SQLT_TYPE_BLOB;
                    } else {
                        col_len1 = (col_len1 - 13) / 2;
                        col_type1 = SQLT_TYPE_TEXT;
                    }
                } else
                    col_len1 = col_data_lens[col_len1];
                ptr1 += vlen;
                int col_len2 = util::read_vint32(ptr2, &vlen);
                int col_type2 = col_len2;
                if (col_len2 >= 12) {
                    if (col_len2 % 2) {
                        col_len2 = (col_len2 - 12) / 2;
                        col_type2 = SQLT_TYPE_BLOB;
                    } else {
                        col_len2 = (col_len2 - 13) / 2;
                        col_type2 = SQLT_TYPE_TEXT;
                    }
                } else
                    col_len2 = col_data_lens[col_len2];
                ptr2 += vlen;
                cmp = compare_col(data_ptr1, col_len1, col_type1,
                            data_ptr2, col_len2, col_type2);
                if (cmp != 0)
                    return cmp;
                cols_compared++;
                if (cols_compared == pk_count)
                    return 0;
                data_ptr1 += col_len1;
                data_ptr2 += col_len2;
            }
            return cmp;
        }

    public:
        uint8_t *master_block;
        int found_pos;
        unsigned long child_addr;
        int pk_count;
        int column_count;
        const std::string column_names;
        const std::string table_name;
        int blk_hdr_len;
        sqlite_index_blaster(int total_col_count, int pk_col_count, 
                const std::string& col_names, const std::string& tbl_name,
                int block_sz = BPT_DEFAULT_BLOCK_SIZE, int cache_sz = 0,
                const char *fname = NULL) : column_count (total_col_count), pk_count (pk_col_count),
                    column_names (col_names), table_name (tbl_name),
                    btree_handler<sqlite_index_blaster>(block_sz, cache_sz, fname, 1, true) {
            init();
        }

        sqlite_index_blaster(uint32_t block_sz, uint8_t *block, bool is_leaf, bool should_init)
                : btree_handler<sqlite_index_blaster>(block_sz, block, is_leaf, should_init) {
            U = block_size - page_resv_bytes;
            X = ((U-12)*64/255)-23;
            M = ((U-12)*32/255)-23;
            master_block = NULL;
        }

        ~sqlite_index_blaster() {
        }

        void init_derived() {
        }

        void cleanup() {
            if (cache_size > 0 && master_block != NULL) {
                uint32_t file_size_in_pages = cache->file_page_count;
                util::write_uint32(master_block + 28, file_size_in_pages);
                cache->write_page(master_block, 0, block_size);
            }
            if (master_block != NULL)
                delete master_block;
        }

        inline void set_current_block_root() {
            set_current_block(root_block);
        }

        inline void set_current_block(uint8_t *m) {
            current_block = m;
            blk_hdr_len = (current_block[0] == 10 || current_block[0] == 13 ? 8 : 12);
        }

        int compare_first_key(const uint8_t *key1, int k_len1,
                            const uint8_t *key2, int k_len2) {
            if (k_len2 < 0) {
                return compare_keys(key1, abs(k_len1), key2, abs(k_len2));
            } else {
                int8_t vlen;
                k_len1 = (util::read_vint32(key1 + 1, &vlen) - 13) / 2;
                key1 += util::read_vint32(key1, &vlen);
                return util::compare(key1, k_len1, key2, k_len2);
            }
            return 0;
        }

        int get_first_key_len() {
            return ((block_size-page_resv_bytes-12)*64/255)-23+5;
        }

        inline int get_header_size() {
            return blk_hdr_len;
        }

        void remove_entry(int pos) {
            del_ptr(pos);
            set_changed(1);
        }

        void remove_found_entry() {
            if (found_pos != -1) {
                del_ptr(found_pos);
                set_changed(1);
            }
            total_size--;
        }

        void del_ptr(int pos) {
            int filled_size = util::read_uint16(current_block + 3);
            uint8_t *kv_idx = current_block + blk_hdr_len + pos * 2;
            int8_t vlen;
            memmove(kv_idx, kv_idx + 2, (filled_size - pos) * 2);
            util::write_uint16(current_block + 3, filled_size - 1);
            // Remove the gaps instead of updating free blocks
            /*
            int rec_len = 0;
            if (!is_leaf())
                rec_len = 4;
            uint8_t *rec_ptr = current_block + read_uint16(kv_idx);
            rec_len += read_vint32(rec_ptr + rec_len, &vlen);
            rec_len += vlen;
            int kv_last_pos = get_kv_last_pos();
            if (rec_ptr != current_block + kv_last_pos)
                memmove(current_block + kv_last_pos + rec_len, current_block + kv_last_pos, rec_ptr - current_block + kv_last_pos);
            kv_last_pos += rec_len;
            set_kv_last_pos(kv_last_pos);*/
        }

        int get_ptr(int pos) {
            return util::read_uint16(current_block + blk_hdr_len + pos * 2);
        }

        void set_ptr(int pos, int ptr) {
            util::write_uint16(current_block + blk_hdr_len + pos * 2, ptr);
        }

        void make_space() {
            int lvl = current_block[0] & 0x1F;
            const int data_size = block_size - get_kv_last_pos();
            uint8_t data_buf[data_size];
            int new_data_len = 0;
            int new_idx;
            int orig_filled_size = filled_size();
            for (new_idx = 0; new_idx < orig_filled_size; new_idx++) {
                int src_idx = get_ptr(new_idx);
                int kv_len = current_block[src_idx];
                kv_len++;
                kv_len += current_block[src_idx + kv_len];
                kv_len++;
                new_data_len += kv_len;
                memcpy(data_buf + data_size - new_data_len, current_block + src_idx, kv_len);
                set_ptr(new_idx, block_size - new_data_len);
            }
            int new_kv_last_pos = block_size - new_data_len;
            memcpy(current_block + new_kv_last_pos, data_buf + data_size - new_data_len, new_data_len);
            //printf("%d, %d\n", data_size, new_data_len);
            set_kv_last_pos(new_kv_last_pos);
            search_current_block();
        }

        void add_first_kv_to_root(uint8_t *first_key, int first_len, 
                unsigned long old_block_addr, unsigned long new_block_addr) {
            uint32_t new_addr32 = (uint32_t) new_block_addr;
            util::write_uint32(current_block + 8, new_addr32 + 1);
            uint8_t addr[9];
            key = first_key;
            key_len = -first_len;
            value = NULL;
            value_len = 0;
            child_addr = old_block_addr + 1;
            //printf("value: %d, value_len1:%d\n", old_page, value_len);
            add_data(0);
        }

        int prepare_kv_to_add_to_parent(uint8_t *first_key, int first_len, unsigned long new_block_addr, uint8_t *addr) {
            key = (uint8_t *) first_key;
            key_len = -first_len;
            value = NULL;
            value_len = 0;
            child_addr = new_block_addr + 1;
            //printf("value: %d, value_len3:%d\n", new_page, value_len);
            int search_result = search_current_block();
            return search_result;
        }

        // TODO: updates only if length of existing record is same as what is updated
        void update_data() {
            int8_t vlen;
            if (key_len > 0) {
                int hdr_len = util::read_vint32(key_at, &vlen);
                uint8_t *raw_key_at = key_at + hdr_len;
                if (memcmp(raw_key_at, key, key_len) != 0)
                    std::cout << "Key not matching for update: " << key << ", len: " << key_len << std::endl;
                raw_key_at += key_len;
                if (hdr_len + key_len + value_len == key_at_len && key_at_len <= X)
                    memcpy(raw_key_at, value, value_len);
            } else {
                int rec_len = -key_len;
                if (rec_len == key_at_len && rec_len <= X)
                    memcpy(key_at, key, rec_len);
            }
        }

        bool is_full(int search_result) {
            int rec_len = abs(key_len) + value_len;
            if (key_len < 0) {
            } else {
                int key_len_vlen = util::get_vlen_of_uint32(key_len * 2 + 13);
                int value_len_vlen = util::get_vlen_of_uint32(value_len * 2 + 13);
                rec_len += key_len_vlen;
                rec_len += value_len_vlen;
                rec_len += util::get_vlen_of_uint32(key_len_vlen + value_len_vlen);
            }
            int on_page_len = (is_leaf() ? 0 : 4);
            int P = rec_len;
            int K = M+((P-M)%(U-4));
            on_page_len += util::get_vlen_of_uint32(rec_len);
            on_page_len += (P <= X ? P : (K <= X ? K : M));
            if (P > X)
                on_page_len += 4;
            int ptr_size = filled_size() + 1;
            ptr_size *= 2;
            if (get_kv_last_pos() <= (blk_hdr_len + ptr_size + on_page_len)) {
                //make_space();
                //if (get_kv_last_pos() <= (blk_hdr_len + ptr_size + rec_len))
                    return true;
            }
            return false;
        }

        uint8_t *allocate_block(int size, int type, int lvl) {
            uint8_t *new_page;
            if (cache_size > 0) {
                new_page = cache->get_new_page(current_block);
                set_block_changed(new_page, size, true);
                if ((cache->file_page_count - 1) * block_size == 1073741824UL) {
                    new_page = cache->get_new_page(current_block);
                    set_block_changed(new_page, size, true);
                }
            }// else
            //    new_page = (uint8_t *) util::aligned_alloc(size);
            if (type != BPT_BLK_TYPE_OVFL) {
                if (type == BPT_BLK_TYPE_INTERIOR)
                    sqlite_common::init_bt_idx_interior(new_page, block_size, page_resv_bytes);
                else
                    sqlite_common::init_bt_idx_leaf(new_page, block_size, page_resv_bytes);
            }
            return new_page;
        }

        uint8_t *split(uint8_t *first_key, int *first_len_ptr) {
            int orig_filled_size = filled_size();
            uint32_t SQLT_NODE_SIZE = block_size;
            int lvl = current_block[0] & 0x1F;
            uint8_t *b = allocate_block(SQLT_NODE_SIZE, is_leaf(), lvl);
            sqlite_index_blaster new_block(SQLT_NODE_SIZE, b, is_leaf(), true);
            set_changed(true);
            new_block.set_changed(true);
            SQLT_NODE_SIZE -= page_resv_bytes;
            int kv_last_pos = get_kv_last_pos();
            int half_kVLen = SQLT_NODE_SIZE - kv_last_pos + 1;
            half_kVLen /= 2;

            // Copy all data to new block in ascending order
            int brk_idx = -1;
            int brk_kv_pos;
            int brk_rec_len;
            uint32_t brk_child_addr;
            int tot_len;
            brk_kv_pos = tot_len = brk_rec_len = brk_child_addr = 0;
            int new_idx;
            for (new_idx = 0; new_idx < orig_filled_size; new_idx++) {
                int src_idx = get_ptr(new_idx);
                int8_t vlen;
                int on_page_len = (is_leaf() ? 0 : 4);
                int P = util::read_vint32(current_block + src_idx + on_page_len, &vlen);
                int K = M+((P-M)%(U-4));
                on_page_len += vlen;
                on_page_len += (P <= X ? P : (K <= X ? K : M));
                if (P > X)
                    on_page_len += 4;
                tot_len += on_page_len;
                if (brk_idx == -1) {
                    if (new_idx > 1 && (tot_len > half_kVLen || new_idx == (orig_filled_size / 2))) {
                        brk_idx = new_idx;
                        *first_len_ptr = P;
                        //if (*first_len_ptr > 2000)
                        //    cout << "GT 200: " << new_idx << ", rec_len: " << on_page_len << ", flp: " << *first_len_ptr << ", src_idx: " << src_idx << endl;
                        memcpy(first_key, current_block + src_idx + (is_leaf() ? 0 : 4) + vlen, on_page_len - (is_leaf() ? 0 : 4) - vlen);
                        brk_rec_len = on_page_len;
                        if (!is_leaf())
                            brk_child_addr = util::read_uint32(current_block + src_idx);
                        brk_kv_pos = kv_last_pos + brk_rec_len;
                    }
                }
                if (brk_idx != new_idx) { // don't copy the middle record, but promote it to prev level
                    memcpy(new_block.current_block + kv_last_pos, current_block + src_idx, on_page_len);
                    new_block.ins_ptr(brk_idx == -1 ? new_idx : new_idx - 1, kv_last_pos + brk_rec_len);
                    kv_last_pos += on_page_len;
                }
            }

            kv_last_pos = get_kv_last_pos();
            if (brk_rec_len) {
                memmove(new_block.current_block + kv_last_pos + brk_rec_len, new_block.current_block + kv_last_pos,
                            SQLT_NODE_SIZE - kv_last_pos - brk_rec_len);
                kv_last_pos += brk_rec_len;
            }
            {
                int diff = (SQLT_NODE_SIZE - brk_kv_pos + brk_rec_len);
                for (new_idx = 0; new_idx < brk_idx; new_idx++) {
                    set_ptr(new_idx, new_block.get_ptr(new_idx) + diff);
                } // Set index of copied first half in old block
                // Copy back first half to old block
                int old_blk_new_len = brk_kv_pos - kv_last_pos;
                memcpy(current_block + SQLT_NODE_SIZE - old_blk_new_len,
                    new_block.current_block + kv_last_pos, old_blk_new_len);
                set_kv_last_pos(SQLT_NODE_SIZE - old_blk_new_len);
                set_filled_size(brk_idx);
                if (!is_leaf()) {
                    uint32_t addr_to_write = brk_child_addr;
                    brk_child_addr = util::read_uint32(current_block + 8);
                    util::write_uint32(current_block + 8, addr_to_write);
                }
            }

            {
                int new_size = orig_filled_size - brk_idx - 1;
                uint8_t *block_ptrs = new_block.current_block + blk_hdr_len;
                memmove(block_ptrs, block_ptrs + (brk_idx << 1), new_size << 1);
                new_block.set_kv_last_pos(brk_kv_pos);
                new_block.set_filled_size(new_size);
                if (!is_leaf())
                    util::write_uint32(new_block.current_block + 8, brk_child_addr);
            }

            return b;
        }

        int write_child_page_addr(uint8_t *ptr, int search_result) {
            if (!is_leaf()) {
                if (search_result == filled_size() && search_result > 0) {
                    unsigned long prev_last_addr = util::read_uint32(current_block + 8);
                    util::write_uint32(current_block + 8, child_addr);
                    child_addr = prev_last_addr;
                }
                if (search_result < filled_size()) {
                    int cur_pos = util::read_uint16(current_block + blk_hdr_len + search_result * 2);
                    uint32_t old_addr = util::read_uint32(current_block + cur_pos);
                    util::write_uint32(current_block + cur_pos, child_addr);
                    child_addr = old_addr;
                }
                util::write_uint32(ptr, child_addr);
                return 4;
            }
            return 0;
        }

        void add_data(int search_result) {

            // P is length of payload or record length
            int P, hdr_len;
            if (key_len < 0) {
                P = -key_len;
                hdr_len = 0;
            } else {
                int key_len_vlen, value_len_vlen;
                P = key_len + value_len;
                key_len_vlen = util::get_vlen_of_uint32(key_len * 2 + 13);
                value_len_vlen = util::get_vlen_of_uint32(value_len * 2 + 13);
                hdr_len = key_len_vlen + value_len_vlen;
                hdr_len += util::get_vlen_of_uint32(hdr_len);
                P += hdr_len;
            }
            // See https://www.sqlite.org/fileformat.html
            int K = M+((P-M)%(U-4));
            int on_bt_page = (P <= X ? P : (K <= X ? K : M));
            int kv_last_pos = get_kv_last_pos();
            kv_last_pos -= on_bt_page;
            kv_last_pos -= util::get_vlen_of_uint32(P);
            if (!is_leaf())
                kv_last_pos -= 4;
            if (P > X)
                kv_last_pos -= 4;
            uint8_t *ptr = current_block + kv_last_pos;
            ptr += write_child_page_addr(ptr, search_result);
            copy_kv_with_overflow(ptr, P, on_bt_page, hdr_len);
            ins_ptr(search_result, kv_last_pos);
            int filled_size = util::read_uint16(current_block + 3);
            set_kv_last_pos(kv_last_pos);
            set_changed(true);

            // if (BPT_MAX_KEY_LEN < key_len)
            //     BPT_MAX_KEY_LEN = key_len;

        }

        void copy_kv_with_overflow(uint8_t *ptr, int P, int on_bt_page, int hdr_len) {
            int k_len, v_len;
            ptr += util::write_vint32(ptr, P);
            if (key_len < 0) {
                if (!is_leaf()) {
                    memcpy(ptr, key, on_bt_page + (P == on_bt_page ? 0 : 4));
                    return;
                }
                k_len = P;
                v_len = 0;
            } else {
                k_len = key_len;
                v_len = value_len;
                ptr += util::write_vint32(ptr, hdr_len);
                ptr += util::write_vint32(ptr, key_len * 2 + 13);
                ptr += util::write_vint32(ptr, value_len * 2 + 13);
            }
            int on_page_remaining = on_bt_page - hdr_len;
            bool copying_on_bt_page = true;
            int key_remaining = k_len;
            int val_remaining = v_len;
            uint8_t *ptr0 = ptr;
            do {
                if (key_remaining > 0) {
                    int to_copy = key_remaining > on_page_remaining ? on_page_remaining : key_remaining;
                    memcpy(ptr, key + k_len - key_remaining, to_copy);
                    ptr += to_copy;
                    key_remaining -= to_copy;
                    on_page_remaining -= to_copy;
                }
                if (val_remaining > 0 && key_remaining <= 0) {
                    int to_copy = val_remaining > on_page_remaining ? on_page_remaining : val_remaining;
                    memcpy(ptr, value + v_len - val_remaining, to_copy);
                    ptr += to_copy;
                    val_remaining -= to_copy;
                }
                if (key_remaining > 0 || val_remaining > 0) {
                    uint32_t new_page_no = cache->get_page_count() + 1;
                    if ((new_page_no - 1) * block_size == 1073741824UL)
                        new_page_no++;
                    util::write_uint32(copying_on_bt_page ? ptr : ptr0, new_page_no);
                    uint8_t *ovfl_ptr = allocate_block(block_size, BPT_BLK_TYPE_OVFL, 0);
                    ptr0 = ptr = ovfl_ptr;
                    ptr += 4;
                    on_page_remaining = U - 4;
                }
                if (!copying_on_bt_page)
                    util::write_uint32(ptr0, 0);
                copying_on_bt_page = false;
            } while (key_remaining > 0 || val_remaining > 0);
        }

        void ins_ptr(int pos, int data_loc) {
            int filled_size = util::read_uint16(current_block + 3);
            uint8_t *kv_idx = current_block + blk_hdr_len + pos * 2;
            memmove(kv_idx + 2, kv_idx, (filled_size - pos) * 2);
            util::write_uint16(current_block + 3, filled_size + 1);
            util::write_uint16(kv_idx, data_loc);
        }

        void add_first_data() {
            add_data(0);
        }

        int filled_size() {
            return util::read_uint16(current_block + 3);
        }

        void set_filled_size(int filled_size) {
            util::write_uint16(current_block + 3, filled_size);
        }

        int get_kv_last_pos() {
            return util::read_uint16(current_block + 5);
        }

        void set_kv_last_pos(int val) {
            if (val == 0)
                val = 65535;
            util::write_uint16(current_block + 5, val);
        }

        int get_level(uint8_t *block, int block_size) {
            return block[block_size - page_resv_bytes] & 0x1F;
        }

        void set_level(uint8_t *block, int block_size, int lvl) {
            block[block_size - page_resv_bytes] = (block[block_size - page_resv_bytes] & 0xE0) + lvl;
        }

        inline uint8_t *get_value_at(int *vlen) {
            int8_t vint_len;
            uint8_t *data_ptr = key_at + util::read_vint32(key_at, &vint_len);
            int hdr_vint_len = vint_len;
            int k_len = (util::read_vint32(key_at + hdr_vint_len, &vint_len) - 13) / 2;
            if (vlen != NULL)
                *vlen = (util::read_vint32(key_at + hdr_vint_len + vint_len, NULL) - 13) / 2;
            return (uint8_t *) data_ptr + k_len;
        }

        void copy_overflow(uint8_t *val, int offset, int len, uint32_t ovfl_page) {
            do {
                uint8_t *ovfl_blk = cache->get_disk_page_in_cache(ovfl_page - 1, current_block);
                ovfl_page = util::read_uint32(ovfl_blk);
                int cur_pos = 4;
                int bytes_remaining = U - 4;
                if (offset > 0) {
                    if (offset > U - 4) {
                        offset -= (U - 4);
                        continue;
                    } else {
                        cur_pos += offset;
                        bytes_remaining -= offset;
                    }
                }
                if (bytes_remaining > 0) {
                    int to_copy = len > bytes_remaining ? bytes_remaining : len;
                    memcpy(val, ovfl_blk + cur_pos, to_copy);
                    val += to_copy;
                    len -= to_copy;
                }
            } while (len > 0 && ovfl_page > 0);
        }

        inline uint8_t *get_child_ptr_pos(int search_result) {
            if (search_result < 0)
                search_result = ~search_result;
            if (search_result == filled_size())
                return current_block + 8;
            return current_block + util::read_uint16(current_block + blk_hdr_len + search_result * 2);
        }

        uint8_t *get_ptr_pos() {
            return current_block + blk_hdr_len;
        }

        inline uint8_t *get_child_ptr(uint8_t *ptr) {
            uint64_t ret = util::read_uint32(ptr);
            return (uint8_t *) ret;
        }

        inline int get_child_page(uint8_t *ptr) {
            return util::read_uint32(ptr) - 1;
        }

        uint8_t *find_split_source(int search_result) {
            return NULL;
        }

        int make_new_rec(uint8_t *ptr, int col_count, const void *values[], 
                const size_t value_lens[] = NULL, const uint8_t types[] = NULL) {
            return sqlite_common::write_new_rec(current_block, -1, 0, col_count, values, value_lens, types, ptr);
        }

        // See .h file for API description
        uint32_t derive_data_len(uint32_t col_type_or_len) {
            if (col_type_or_len >= 12) {
                if (col_type_or_len % 2)
                    return (col_type_or_len - 13)/2;
                return (col_type_or_len - 12)/2; 
            } else if (col_type_or_len < 10)
                return col_data_lens[col_type_or_len];
            return 0;
        }

        // See .h file for API description
        uint32_t derive_col_type(uint32_t col_type_or_len) {
            if (col_type_or_len >= 12) {
                if (col_type_or_len % 2)
                    return SQLT_TYPE_TEXT;
                return SQLT_TYPE_BLOB;
            } else if (col_type_or_len < 10)
                return col_type_or_len;
            return 0;
        }

        int read_col(int which_col, uint8_t *rec, int rec_len, void *out) {
            int col_type_or_len, col_len, col_type;
            uint8_t *data_ptr = locate_col(which_col, rec, col_type_or_len, col_len, col_type);
            if (data_ptr == NULL)
                return SQLT_RES_MALFORMED;
            switch (col_type) {
                case SQLT_TYPE_BLOB:
                case SQLT_TYPE_TEXT:
                    memcpy(out, data_ptr, col_len);
                    return col_len;
                case SQLT_TYPE_NULL:
                    return col_type_or_len;
                case SQLT_TYPE_INT0:
                    *((int8_t *) out) = 0;
                    return col_len;
                case SQLT_TYPE_INT1:
                    *((int8_t *) out) = 1;
                    return col_len;
                case SQLT_TYPE_INT8:
                    *((int8_t *) out) = *data_ptr;
                    return col_len;
                case SQLT_TYPE_INT16:
                    *((int16_t *) out) = util::read_uint16(data_ptr);
                    return col_len;
                case SQLT_TYPE_INT24:
                    *((int32_t *) out) = util::read_int24(data_ptr);
                    return col_len;
                case SQLT_TYPE_INT32:
                    *((int32_t *) out) = util::read_uint32(data_ptr);
                    return col_len;
                case SQLT_TYPE_INT48:
                    *((int64_t *) out) = util::read_int48(data_ptr);
                    return col_len;
                case SQLT_TYPE_INT64:
                    *((int64_t *) out) = util::read_uint64(data_ptr);
                    return col_len;
                case SQLT_TYPE_REAL:
                    *((double *) out) = util::read_double(data_ptr);
                    return col_len;
            }
            return SQLT_RES_MALFORMED;
        }

        int search_current_block(bptree_iter_ctx *ctx = NULL) {
            int middle, first, filled_sz, cmp;
            int8_t vlen;
            found_pos = -1;
            first = 0;
            filled_sz = util::read_uint16(current_block + 3);
            while (first < filled_sz) {
                middle = (first + filled_sz) >> 1;
                key_at = current_block + util::read_uint16(current_block + blk_hdr_len + middle * 2);
                if (!is_leaf())
                    key_at += 4;
                key_at_len = util::read_vint32(key_at, &vlen);
                key_at += vlen;
                if (key_len < 0) {
                    cmp = compare_keys(key_at, key_at_len, key, abs(key_len));
                } else {
                    uint8_t *raw_key_at = key_at + util::read_vint32(key_at, &vlen);
                    cmp = util::compare(raw_key_at, (util::read_vint32(key_at + vlen, NULL) - 13) / 2,
                                        key, key_len);
                }
                if (cmp < 0)
                    first = middle + 1;
                else if (cmp > 0)
                    filled_sz = middle;
                else {
                    if (ctx) {
                        ctx->found_page_idx = ctx->last_page_lvl;
                        ctx->found_page_pos = middle;
                    }
                    return middle;
                }
            }
            if (ctx) {
                ctx->found_page_idx = ctx->last_page_lvl;
                ctx->found_page_pos = ~filled_sz;
            }
            return ~filled_sz;
        }

        void set_changed(bool is_changed) {
            if (is_changed)
                current_block[block_size - page_resv_bytes] |= 0x40;
            else
                current_block[block_size - page_resv_bytes] &= 0xBF;
        }

        bool is_changed() {
            return current_block[block_size - page_resv_bytes] & 0x40;
        }

        static void set_block_changed(uint8_t *block, int block_size, bool is_changed) {
            if (is_changed)
                block[block_size - page_resv_bytes] |= 0x40;
            else
                block[block_size - page_resv_bytes] &= 0xBF;
        }

        static bool is_block_changed(uint8_t *block, int block_size) {
            return block[block_size - page_resv_bytes] & 0x40;
        }

        inline bool is_leaf() {
            return current_block[0] > 9;
        }

        void set_leaf(char is_leaf) {
            if (is_leaf)
                sqlite_common::init_bt_idx_leaf(current_block, block_size, page_resv_bytes);
            else
                sqlite_common::init_bt_idx_interior(current_block, block_size, page_resv_bytes);
            blk_hdr_len = (current_block[0] == 10 || current_block[0] == 13 ? 8 : 12);
        }

        void init_current_block() {
            //memset(current_block, '\0', BFOS_NODE_SIZE);
            //cout << "Tree init block" << endl;
            if (!is_block_given) {
            }
        }

        void copy_value(uint8_t *val, int *p_value_len) {
            int8_t vlen;
            int P = key_at_len;
            int K = M+((P-M)%(U-4));
            int on_bt_page = (P <= X ? P : (K <= X ? K : M));
            if (key_len < 0) {
                *p_value_len = key_at_len;
                memcpy(val, key_at, on_bt_page);
                if (P > on_bt_page)
                    copy_overflow(val + on_bt_page, 0, P - on_bt_page, util::read_uint32(key_at + on_bt_page));
            } else {
                int hdr_len = util::read_vint32(key_at, &vlen);
                uint8_t *raw_val_at = key_at + hdr_len;
                int8_t key_len_vlen;
                int k_len = (util::read_vint32(key_at + vlen, &key_len_vlen) - 13) / 2;
                *p_value_len = (util::read_vint32(key_at + vlen + key_len_vlen, NULL) - 13) / 2;
                if (hdr_len + k_len <= on_bt_page) {
                    raw_val_at += k_len;
                    int val_len_on_bt = on_bt_page - k_len - hdr_len;
                    memcpy(val, raw_val_at, val_len_on_bt);
                    if (*p_value_len - val_len_on_bt > 0)
                        copy_overflow(val + val_len_on_bt, 0, *p_value_len - val_len_on_bt, util::read_uint32(key_at + on_bt_page));
                } else {
                    if (*p_value_len > 0)
                        copy_overflow(val, hdr_len + k_len - on_bt_page, *p_value_len, util::read_uint32(key_at + on_bt_page));
                }
            }
        }

        // bool next(bptree_iter_ctx *ctx, int *in_size_out_val_len = NULL, uint8_t *val = NULL) {
        //     ctx->found_page_idx;
        //     ctx->found_page_pos;
        //     ctx->last_page_lvl;
        //     ctx->pages;
        //     if (val != NULL)
        //         copy_value(val, in_size_out_val_len);
        //     return true;
        // }

};

#undef page_resv_bytes
#undef BPT_LEAF0_LVL
#undef BPT_STAGING_LVL
#undef BPT_PARENT0_LVL

#undef BPT_BLK_TYPE_INTERIOR
#undef BPT_BLK_TYPE_LEAF
#undef BPT_BLK_TYPE_OVFL

#undef BPT_DEFAULT_BLOCK_SIZE

#undef descendant
#undef BPT_MAX_LVL_COUNT

#endif
