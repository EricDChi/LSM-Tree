#define delete_sign "~DELETE~"

#define Magic 0xff

#define max_sst_size 16 * 1024

#define header_size 32

#define bloomfilter_size 1 * 1024

#define batch_size (max_sst_size - header_size - bloomfilter_size) / 20

#define vlog_entry_header_size 15