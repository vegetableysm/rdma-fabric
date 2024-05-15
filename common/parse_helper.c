#include "shared.h"


void ft_parseinfo(int op, char *optarg, struct fi_info *hints,
		  struct ft_opts *opts)
{
	switch (op) {
	case 'f':
		if (!hints->fabric_attr) {
			hints->fabric_attr = malloc(sizeof *(hints->fabric_attr));
			if (!hints->fabric_attr) {
				perror("malloc");
				exit(EXIT_FAILURE);
			}
		}
		hints->fabric_attr->name = strdup(optarg);
		break;
	case 'd':
		if (!hints->domain_attr) {
			hints->domain_attr = malloc(sizeof *(hints->domain_attr));
			if (!hints->domain_attr) {
				perror("malloc");
				exit(EXIT_FAILURE);
			}
		}
		hints->domain_attr->name = strdup(optarg);
		break;
	case 'p':
		if (!hints->fabric_attr) {
			hints->fabric_attr = malloc(sizeof *(hints->fabric_attr));
			if (!hints->fabric_attr) {
				perror("malloc");
				exit(EXIT_FAILURE);
			}
		}
		hints->fabric_attr->prov_name = strdup(optarg);
		break;
	case 'e':
		if (!strncasecmp("msg", optarg, 3))
			hints->ep_attr->type = FI_EP_MSG;
		if (!strncasecmp("rdm", optarg, 3))
			hints->ep_attr->type = FI_EP_RDM;
		if (!strncasecmp("dgram", optarg, 5))
			hints->ep_attr->type = FI_EP_DGRAM;
		break;
	case 'M':
		if (!strncasecmp("mr_local", optarg, 8))
			opts->mr_mode &= ~FI_MR_LOCAL;
		break;
	case 'K':
		opts->options |= FT_OPT_FORK_CHILD;
		break;
	default:
		ft_parse_hmem_opts(op, optarg, opts);
		/* let getopt handle unknown opts*/
		break;
	}
}

void ft_parse_addr_opts(int op, char *optarg, struct ft_opts *opts)
{
	switch (op) {
	case 's':
		opts->src_addr = optarg;
		break;
	case 'B':
		opts->src_port = optarg;
		break;
	case 'P':
		opts->dst_port = optarg;
		break;
	case 'b':
		opts->options |= FT_OPT_OOB_SYNC;
		/* fall through */
	case 'E':
		opts->options |= FT_OPT_OOB_ADDR_EXCH;
		if (optarg && strlen(optarg) > 1)
			opts->oob_port = optarg + 1;
		else
			opts->oob_port = default_oob_port;
		if (!opts->oob_addr)
			opts->options |= FT_OPT_ADDR_IS_OOB;
		break;
	case 'F':
		if (!strncasecmp("fi_addr_str", optarg, 11))
			opts->address_format = FI_ADDR_STR;
		else if (!strncasecmp("fi_sockaddr_in6", optarg, 15))
			opts->address_format = FI_SOCKADDR_IN6;
		else if (!strncasecmp("fi_sockaddr_in", optarg, 14))
			opts->address_format = FI_SOCKADDR_IN;
		else if (!strncasecmp("fi_sockaddr_ib", optarg, 14))
			opts->address_format = FI_SOCKADDR_IB;
		else if (!strncasecmp("fi_sockaddr", optarg, 11)) /* keep me last */
			opts->address_format = FI_SOCKADDR;
		break;
	case 'C':
		opts->options |= FT_OPT_SERVER_PERSIST;
		opts->num_connections = atoi(optarg);
		break;
	case 'O':
		opts->oob_addr = optarg;
		opts->options &= ~FT_OPT_ADDR_IS_OOB;
		break;
	default:
		/* let getopt handle unknown opts*/
		break;
	}
}

void ft_parse_hmem_opts(int op, char *optarg, struct ft_opts *opts)
{
	switch (op) {
	case 'D':
		if (!strncasecmp("ze", optarg, 2))
			opts->iface = FI_HMEM_ZE;
		else if (!strncasecmp("cuda", optarg, 4))
			opts->iface = FI_HMEM_CUDA;
		else if (!strncasecmp("neuron", optarg, 6))
			opts->iface = FI_HMEM_NEURON;
		else
			printf("Unsupported interface\n");
		opts->options |= FT_OPT_ENABLE_HMEM | FT_OPT_USE_DEVICE;
		break;
	case 'i':
		opts->device = atoi(optarg);
		break;
	case 'H':
		opts->options |= FT_OPT_ENABLE_HMEM;
		break;
	case 'R':
		opts->options |= FT_OPT_REG_DMABUF_MR;
		break;
	default:
		/* Let getopt handle unknown opts*/
		break;
	}
}

void ft_parse_opts_range(char* optarg)
{
	size_t start, inc, end;
	int i, ret;

	ret = sscanf(optarg, "r:%zd,%zd,%zd", &start, &inc, &end);
	if (ret != 3) {
		perror("sscanf");
		exit(EXIT_FAILURE);
	}
	assert(end >= start && inc > 0);
	test_cnt = (end - start) / inc + 1;
	user_test_sizes = calloc(test_cnt, sizeof(*user_test_sizes));
	if (!user_test_sizes) {
		perror("calloc");
		exit(EXIT_FAILURE);
	}
	for (i = 0; i < test_cnt && i < end; i++) {
		user_test_sizes[i].size = start + (i * inc);
		user_test_sizes[i].enable_flags = 0;
	}
	test_size = user_test_sizes;
}

void ft_parse_opts_list(char* optarg)
{
	int i, ret;
	char *token;
	char *saveptr;

	optarg += 2; // remove 'l:'
	test_cnt = 1;
	for (i = 0; optarg[i] != '\0'; i++) {
		test_cnt += optarg[i] == ',';
	}
	user_test_sizes = calloc(test_cnt, sizeof(*user_test_sizes));
	if (!user_test_sizes) {
		perror("calloc");
		exit(EXIT_FAILURE);
	}

	token = strtok_r(optarg, ",", &saveptr);
	test_cnt = 0;
	while (token != NULL) {
		ret = sscanf(token, "%zu", &user_test_sizes[test_cnt].size);
		if (ret != 1) {
			fprintf(stderr, "Cannot parse integer \"%s\" in list.\n",token);
			exit(EXIT_FAILURE);
		}
		test_cnt++;
		token = strtok_r(NULL, ",", &saveptr);
	}
	test_size = user_test_sizes;
}

void ft_parsecsopts(int op, char *optarg, struct ft_opts *opts)
{
	ft_parse_addr_opts(op, optarg, opts);

	switch (op) {
	case 'I':
		opts->options |= FT_OPT_ITER;
		opts->iterations = atoi(optarg);
		break;
	case 'Q':
		opts->options |= FT_OPT_DOMAIN_EQ;
		break;
	case 'S':
		if (!strncasecmp("all", optarg, 3)) {
			opts->sizes_enabled = FT_ENABLE_SIZES;
		} else if (!strncasecmp("r:", optarg, 2)){
			opts->sizes_enabled = FT_ENABLE_SIZES;
			ft_parse_opts_range(optarg);
		} else if (!strncasecmp("l:", optarg, 2)){
			opts->sizes_enabled = FT_ENABLE_SIZES;
			ft_parse_opts_list(optarg);
		} else {
			opts->options |= FT_OPT_SIZE;
			opts->transfer_size = atol(optarg);
		}
		break;
	case 'm':
		opts->machr = 1;
		break;
	case 'c':
		if (!strncasecmp("sread", optarg, 5))
			opts->comp_method = FT_COMP_SREAD;
		else if (!strncasecmp("fd", optarg, 2))
			opts->comp_method = FT_COMP_WAIT_FD;
		else if (!strncasecmp("yield", optarg, 5))
			opts->comp_method = FT_COMP_YIELD;
		break;
	case 'a':
		opts->av_name = optarg;
		break;
	case 'w':
		opts->warmup_iterations = atoi(optarg);
		break;
	case 'l':
		opts->options |= FT_OPT_ALIGN;
		break;
	default:
		/* let getopt handle unknown opts*/
		break;
	}
}

int ft_parse_api_opts(int op, char *optarg, struct fi_info *hints,
		      struct ft_opts *opts)
{
	switch (op) {
	case 'o':
		if (!strcasecmp(optarg, "read")) {
			hints->caps |= FI_READ | FI_REMOTE_READ;
			opts->rma_op = FT_RMA_READ;
		} else if (!strcasecmp(optarg, "writedata")) {
			hints->caps |= FI_WRITE | FI_REMOTE_WRITE;
			hints->mode |= FI_RX_CQ_DATA;
			hints->domain_attr->cq_data_size = 4;
			opts->rma_op = FT_RMA_WRITEDATA;
			opts->cqdata_op = FT_CQDATA_WRITEDATA;
			cq_attr.format = FI_CQ_FORMAT_DATA;
		} else if (!strcasecmp(optarg, "senddata")) {
			hints->mode |= FI_RX_CQ_DATA;
			hints->domain_attr->cq_data_size = 4;
			opts->cqdata_op = FT_CQDATA_SENDDATA;
			cq_attr.format = FI_CQ_FORMAT_DATA;
		} else if (!strcasecmp(optarg, "write")) {
			hints->caps |= FI_WRITE | FI_REMOTE_WRITE;
			opts->rma_op = FT_RMA_WRITE;
		} else if (!strcasecmp(optarg, "msg")) {
			hints->caps |= FI_MSG;
		} else if (!strcasecmp(optarg, "tagged")) {
			hints->caps |= FI_TAGGED;
		} else {
			fprintf(stderr, "Invalid operation type: \"%s\"."
				"Usage:\n-o <op>\top: "
				"read|write|writedata|msg|tagged\n", optarg);
			return EXIT_FAILURE;
		}
		break;
	default:
		/* let getopt handle unknown opts*/
		break;
	}
	return 0;
}
