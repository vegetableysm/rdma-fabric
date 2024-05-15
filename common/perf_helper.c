#include "shared.h"

int64_t get_elapsed(const struct timespec *b, const struct timespec *a,
		    enum precision p)
{
	int64_t elapsed;

	elapsed = difftime(a->tv_sec, b->tv_sec) * 1000 * 1000 * 1000;
	elapsed += a->tv_nsec - b->tv_nsec;
	return elapsed / p;
}

void show_perf(char *name, size_t tsize, int iters, struct timespec *start,
		struct timespec *end, int xfers_per_iter)
{
	static int header = 1;
	char str[FT_STR_LEN];
	int64_t elapsed = get_elapsed(start, end, MICRO);
	long long bytes = (long long) iters * tsize * xfers_per_iter;
	float usec_per_xfer;

	if (name) {
		if (header) {
			printf("%-50s%-8s%-8s%-8s%8s %10s%13s%13s\n",
					"name", "bytes", "iters",
					"total", "time", "MB/sec",
					"usec/xfer", "Mxfers/sec");
			header = 0;
		}

		printf("%-50s", name);
	} else {
		if (header) {
			printf("%-8s%-8s%-8s%8s %10s%13s%13s\n",
					"bytes", "iters", "total",
					"time", "MB/sec", "usec/xfer",
					"Mxfers/sec");
			header = 0;
		}
	}

	printf("%-8s", size_str(str, tsize));

	printf("%-8s", cnt_str(str, iters));

	printf("%-8s", size_str(str, bytes));

	usec_per_xfer = ((float)elapsed / iters / xfers_per_iter);
	printf("%8.2fs%10.2f%11.2f%11.2f\n",
		elapsed / 1000000.0, bytes / (1.0 * elapsed),
		usec_per_xfer, 1.0/usec_per_xfer);
}

void show_perf_mr(size_t tsize, int iters, struct timespec *start,
		  struct timespec *end, int xfers_per_iter, int argc, char *argv[])
{
	static int header = 1;
	int64_t elapsed = get_elapsed(start, end, MICRO);
	long long total = (long long) iters * tsize * xfers_per_iter;
	int i;
	float usec_per_xfer;

	if (header) {
		printf("---\n");

		for (i = 0; i < argc; ++i)
			printf("%s ", argv[i]);

		printf(":\n");
		header = 0;
	}

	usec_per_xfer = ((float)elapsed / iters / xfers_per_iter);

	printf("- { ");
	printf("xfer_size: %zu, ", tsize);
	printf("iterations: %d, ", iters);
	printf("total: %lld, ", total);
	printf("time: %f, ", elapsed / 1000000.0);
	printf("MB/sec: %f, ", (total) / (1.0 * elapsed));
	printf("usec/xfer: %f, ", usec_per_xfer);
	printf("Mxfers/sec: %f", 1.0/usec_per_xfer);
	printf(" }\n");
}

void ft_addr_usage()
{
	FT_PRINT_OPTS_USAGE("-B <src_port>", "non default source port number");
	FT_PRINT_OPTS_USAGE("-P <dst_port>", "non default destination port number");
	FT_PRINT_OPTS_USAGE("-s <address>", "source address");
	FT_PRINT_OPTS_USAGE("-b[=<oob_port>]", "enable out-of-band address exchange and "
			"synchronization over the, optional, port");
	FT_PRINT_OPTS_USAGE("-E[=<oob_port>]", "enable out-of-band address exchange only "
			"over the, optional, port");
	FT_PRINT_OPTS_USAGE("-C <number>", "simultaneous connections to server");
	FT_PRINT_OPTS_USAGE("-O <addr>", "use the provided addr for out of band");
	FT_PRINT_OPTS_USAGE("-F <addr_format>", "Address format (default:FI_FORMAT_UNSPEC)");
}

void ft_usage(char *name, char *desc)
{
	fprintf(stderr, "Usage:\n");
	fprintf(stderr, "  %s [OPTIONS]\t\tstart server\n", name);
	fprintf(stderr, "  %s [OPTIONS] <host>\tconnect to server\n", name);

	if (desc)
		fprintf(stderr, "\n%s\n", desc);

	fprintf(stderr, "\nOptions:\n");
	ft_addr_usage();
	FT_PRINT_OPTS_USAGE("-f <fabric>", "fabric name");
	FT_PRINT_OPTS_USAGE("-d <domain>", "domain name");
	FT_PRINT_OPTS_USAGE("-p <provider>", "specific provider name eg sockets, verbs");
	FT_PRINT_OPTS_USAGE("-e <ep_type>", "Endpoint type: msg|rdm|dgram (default:rdm)");
	FT_PRINT_OPTS_USAGE("", "Only the following tests support this option for now:");
	FT_PRINT_OPTS_USAGE("", "fi_rma_bw");
	FT_PRINT_OPTS_USAGE("", "fi_shared_ctx");
	FT_PRINT_OPTS_USAGE("", "fi_multi_mr");
	FT_PRINT_OPTS_USAGE("", "fi_multi_ep");
	FT_PRINT_OPTS_USAGE("", "fi_recv_cancel");
	FT_PRINT_OPTS_USAGE("", "fi_unexpected_msg");
	FT_PRINT_OPTS_USAGE("", "fi_resmgmt_test");
	FT_PRINT_OPTS_USAGE("", "fi_bw");
	FT_PRINT_OPTS_USAGE("-U", "run fabtests with FI_DELIVERY_COMPLETE set");
	FT_PRINT_OPTS_USAGE("", "Only the following tests support this option for now:");
	FT_PRINT_OPTS_USAGE("", "fi_bw");
	FT_PRINT_OPTS_USAGE("", "fi_rdm");
	FT_PRINT_OPTS_USAGE("", "fi_rdm_atomic");
	FT_PRINT_OPTS_USAGE("", "fi_rdm_pingpong");
	FT_PRINT_OPTS_USAGE("", "fi_rdm_tagged_bw");
	FT_PRINT_OPTS_USAGE("", "fi_rdm_tagged_pingpong");
	FT_PRINT_OPTS_USAGE("", "fi_rma_bw");
	FT_PRINT_OPTS_USAGE("-M <mode>", "Disable mode bit from test");
	FT_PRINT_OPTS_USAGE("-K", "fork a child process after initializing endpoint");
	FT_PRINT_OPTS_USAGE("", "mr_local");
	FT_PRINT_OPTS_USAGE("-a <address vector name>", "name of address vector");
	FT_PRINT_OPTS_USAGE("-h", "display this help output");

	return;
}

void ft_hmem_usage()
{
	FT_PRINT_OPTS_USAGE("-D <device_iface>", "Specify device interface: "
			    "e.g. cuda, ze, neuron (default: None). "
			    "Automatically enables FI_HMEM (-H)");
	FT_PRINT_OPTS_USAGE("-i <device_id>", "Specify which device to use (default: 0)");
	FT_PRINT_OPTS_USAGE("-H", "Enable provider FI_HMEM support");
	FT_PRINT_OPTS_USAGE("-R", "Register HMEM memory with fi_mr_dmabuf API");
}

void ft_mcusage(char *name, char *desc)
{
	fprintf(stderr, "Usage:\n");
	fprintf(stderr, "  %s [OPTIONS] -M <mcast_addr>\tstart listener\n", name);
	fprintf(stderr, "  %s [OPTIONS] <mcast_addr>\tsend to group\n", name);

	if (desc)
		fprintf(stderr, "\n%s\n", desc);

	fprintf(stderr, "\nOptions:\n");
	ft_addr_usage();
	FT_PRINT_OPTS_USAGE("-f <fabric>", "fabric name");
	FT_PRINT_OPTS_USAGE("-d <domain>", "domain name");
	FT_PRINT_OPTS_USAGE("-p <provider>", "specific provider name eg sockets, verbs");
	ft_hmem_usage();
	FT_PRINT_OPTS_USAGE("-h", "display this help output");

	return;
}

void ft_csusage(char *name, char *desc)
{
	ft_usage(name, desc);
	FT_PRINT_OPTS_USAGE("-I <number>", "number of iterations");
	FT_PRINT_OPTS_USAGE("-Q", "bind EQ to domain (vs. endpoint)");
	FT_PRINT_OPTS_USAGE("-w <number>", "number of warmup iterations");
	FT_PRINT_OPTS_USAGE("-S <size>", "specific transfer size or "
			    "a range of sizes (syntax r:start,inc,end) or "
			    "a list of sizes (syntax l:1,1,2,3,5,...) or 'all'");
	FT_PRINT_OPTS_USAGE("-l", "align transmit and receive buffers to page size");
	FT_PRINT_OPTS_USAGE("-m", "machine readable output");
	ft_hmem_usage();
	FT_PRINT_OPTS_USAGE("-t <type>", "completion type [queue, counter]");
	FT_PRINT_OPTS_USAGE("-c <method>", "completion method [spin, sread, fd, yield]");
	FT_PRINT_OPTS_USAGE("-h", "display this help output");

	return;
}

char *size_str(char str[FT_STR_LEN], long long size)
{
	long long base, fraction = 0;
	char mag;

	memset(str, '\0', FT_STR_LEN);

	if (size >= (1 << 30)) {
		base = 1 << 30;
		mag = 'g';
	} else if (size >= (1 << 20)) {
		base = 1 << 20;
		mag = 'm';
	} else if (size >= (1 << 10)) {
		base = 1 << 10;
		mag = 'k';
	} else {
		base = 1;
		mag = '\0';
	}

	if (size / base < 10)
		fraction = (size % base) * 10 / base;

	if (fraction)
		snprintf(str, FT_STR_LEN, "%lld.%lld%c", size / base, fraction, mag);
	else
		snprintf(str, FT_STR_LEN, "%lld%c", size / base, mag);

	return str;
}

char *cnt_str(char str[FT_STR_LEN], long long cnt)
{
	if (cnt >= 1000000000)
		snprintf(str, FT_STR_LEN, "%lldb", cnt / 1000000000);
	else if (cnt >= 1000000)
		snprintf(str, FT_STR_LEN, "%lldm", cnt / 1000000);
	else if (cnt >= 1000)
		snprintf(str, FT_STR_LEN, "%lldk", cnt / 1000);
	else
		snprintf(str, FT_STR_LEN, "%lld", cnt);

	return str;
}

int size_to_count(int size)
{
	if (size >= (1 << 20))
		return (opts.options & FT_OPT_BW) ? 200 : 100;
	else if (size >= (1 << 16))
		return (opts.options & FT_OPT_BW) ? 2000 : 1000;
	else
		return (opts.options & FT_OPT_BW) ? 20000: 10000;
}

void ft_longopts_usage()
{
	FT_PRINT_OPTS_USAGE("--pin-core <core_list>",
		"Specify which cores to pin process to using a\n"
		"a comma-separated list format, e.g.: 0,2-4.\n"
		"Disabled by default.");
	FT_PRINT_OPTS_USAGE("--timeout <seconds>",
		"Overrides default timeout for test specific transfers.");
	FT_PRINT_OPTS_USAGE("--debug-assert",
		"Replace asserts with while loops to force process to\n"
		"spin until a debugger can be attached.");
	FT_PRINT_OPTS_USAGE("--data-progress <progress_model>",
		"manual, or auto");
	FT_PRINT_OPTS_USAGE("--control-progress <progress_model>",
		"manual, auto, or unified");
}

int lopt_idx = 0;
int debug_assert;

struct option long_opts[] = {
	{"pin-core", required_argument, NULL, LONG_OPT_PIN_CORE},
	{"timeout", required_argument, NULL, LONG_OPT_TIMEOUT},
	{"debug-assert", no_argument, &debug_assert, LONG_OPT_DEBUG_ASSERT},
	{"data-progress", required_argument, NULL, LONG_OPT_DATA_PROGRESS},
	{"control-progress", required_argument, NULL, LONG_OPT_CONTROL_PROGRESS},
	{NULL, 0, NULL, 0},
};

static int ft_parse_pin_core_opt(char *optarg)
{
	return 0;
}

int ft_parse_progress_model_string(char* progress_str)
{
	int ret = -1;

	if (!strcasecmp("manual", progress_str))
		ret = FI_PROGRESS_MANUAL;
	else if (!strcasecmp("auto", progress_str))
		ret = FI_PROGRESS_AUTO;
	else if (!strcasecmp("unified", progress_str))
		ret = FI_PROGRESS_CONTROL_UNIFIED;

	return ret;
}

int ft_parse_long_opts(int op, char *optarg)
{
	switch (op) {
	case LONG_OPT_PIN_CORE:
		return ft_parse_pin_core_opt(optarg);
	case LONG_OPT_TIMEOUT:
		timeout = atoi(optarg);
		return 0;
	case LONG_OPT_DEBUG_ASSERT:
		return 0;
	case LONG_OPT_DATA_PROGRESS:
		hints->domain_attr->data_progress = ft_parse_progress_model_string(optarg);
		if (hints->domain_attr->data_progress == -1)
			return EXIT_FAILURE;
		return 0;
	case LONG_OPT_CONTROL_PROGRESS:
		hints->domain_attr->control_progress = ft_parse_progress_model_string(optarg);
		if (hints->domain_attr->control_progress == -1)
			return EXIT_FAILURE;
		return 0;
	default:
		return EXIT_FAILURE;
	}
}

