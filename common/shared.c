/*
 * Copyright (c) 2013-2018 Intel Corporation.  All rights reserved.
 * Copyright (c) 2016 Cray Inc.  All rights reserved.
 * Copyright (c) 2014-2017, Cisco Systems, Inc. All rights reserved.
 * Copyright (c) 2021 Amazon.com, Inc. or its affiliates. All rights reserved.
 *
 * This software is available to you under the BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#define _GNU_SOURCE
#include <assert.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sched.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sched.h>
#include <pthread.h>

#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_tagged.h>
#include <rdma/fi_atomic.h>
#include <rdma/fi_collective.h>

#include "shared.h"

struct fi_info *fi_pep, *fi, *hints;
struct fid_fabric *fabric;
struct fid_wait *waitset;
struct fid_domain *domain;
struct fid_poll *pollset;
struct fid_pep *pep;
struct fid_ep *ep, *alias_ep;
struct fid_cq *txcq, *rxcq;
struct fid_cntr *txcntr, *rxcntr, *rma_cntr;

struct fid_ep *srx;
struct fid_stx *stx;
struct fid_mr *mr;
void *mr_desc = NULL;
struct fid_av *av = NULL;
struct fid_eq *eq;
struct fid_mc *mc;

struct fid_mr no_mr;
struct fi_context tx_ctx, rx_ctx;
struct ft_context *tx_ctx_arr = NULL, *rx_ctx_arr = NULL;
uint64_t remote_cq_data = 0;

uint64_t tx_seq, rx_seq, tx_cq_cntr, rx_cq_cntr;
int (*ft_mr_alloc_func)(void);
uint64_t ft_tag = 0;
pid_t ft_child_pid = 0;

fi_addr_t remote_fi_addr = FI_ADDR_UNSPEC;
char *buf = NULL, *tx_buf, *rx_buf;
/*
 * dev_host_buf are used by ft_fill_buf() to stage data sent over wire,
 * when tx_buf is on device memory.
 */
void *dev_host_buf = NULL;

char **tx_mr_bufs = NULL, **rx_mr_bufs = NULL;
size_t buf_size, tx_buf_size, rx_buf_size;
size_t tx_size, rx_size, tx_mr_size, rx_mr_size;
int rx_fd = -1, tx_fd = -1;
char default_port[8] = "9228";
char default_oob_port[8] = "3000";
// const char *greeting = "Hello from Client!";


char test_name[50] = "custom";
int timeout = -1;
struct timespec start, end;

int listen_sock = -1;
int oob_sock = -1;

struct fi_rma_iov remote;

struct ft_opts opts;

struct test_size_param def_test_sizes[] = {
	{ 1 <<  0, 0 },
	{ 1 <<  1, 0 }, { (1 <<  1) + (1 <<  0), 0 },
	{ 1 <<  2, 0 }, { (1 <<  2) + (1 <<  1), 0 },
	{ 1 <<  3, 0 }, { (1 <<  3) + (1 <<  2), 0 },
	{ 1 <<  4, 0 }, { (1 <<  4) + (1 <<  3), 0 },
	{ 1 <<  5, 0 }, { (1 <<  5) + (1 <<  4), 0 },
	{ 1 <<  6, FT_DEFAULT_SIZE }, { (1 <<  6) + (1 <<  5), 0 },
	{ 1 <<  7, 0 }, { (1 <<  7) + (1 <<  6), 0 },
	{ 1 <<  8, FT_DEFAULT_SIZE }, { (1 <<  8) + (1 <<  7), 0 },
	{ 1 <<  9, 0 }, { (1 <<  9) + (1 <<  8), 0 },
	{ 1 << 10, FT_DEFAULT_SIZE }, { (1 << 10) + (1 <<  9), 0 },
	{ 1 << 11, 0 }, { (1 << 11) + (1 << 10), 0 },
	{ 1 << 12, FT_DEFAULT_SIZE }, { (1 << 12) + (1 << 11), 0 },
	{ 1 << 13, 0 }, { (1 << 13) + (1 << 12), 0 },
	{ 1 << 14, 0 }, { (1 << 14) + (1 << 13), 0 },
	{ 1 << 15, 0 }, { (1 << 15) + (1 << 14), 0 },
	{ 1 << 16, FT_DEFAULT_SIZE }, { (1 << 16) + (1 << 15), 0 },
	{ 1 << 17, 0 }, { (1 << 17) + (1 << 16), 0 },
	{ 1 << 18, 0 }, { (1 << 18) + (1 << 17), 0 },
	{ 1 << 19, 0 }, { (1 << 19) + (1 << 18), 0 },
	{ 1 << 20, FT_DEFAULT_SIZE }, { (1 << 20) + (1 << 19), 0 },
	{ 1 << 21, 0 }, { (1 << 21) + (1 << 20), 0 },
	{ 1 << 22, 0 }, { (1 << 22) + (1 << 21), 0 },
	{ 1 << 23, 0 },
};

unsigned int test_cnt = (sizeof def_test_sizes / sizeof def_test_sizes[0]);

struct test_size_param *test_size = def_test_sizes;
// /* range of messages is dynamically allocated */
struct test_size_param *user_test_sizes;

static const char integ_alphabet[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
static const int integ_alphabet_length = (sizeof(integ_alphabet)/sizeof(*integ_alphabet)) - 1;


int ft_poll_fd(int fd, int timeout)
{
	struct pollfd fds;
	int ret;

	fds.fd = fd;
	fds.events = POLLIN;
	ret = poll(&fds, 1, timeout);
	if (ret == -1) {
		FT_PRINTERR("poll", -errno);
		ret = -errno;
	} else if (!ret) {
		ret = -FI_EAGAIN;
	} else {
		ret = 0;
	}
	return ret;
}

size_t ft_tx_prefix_size(void)
{
	return (fi->tx_attr->mode & FI_MSG_PREFIX) ?
		fi->ep_attr->msg_prefix_size : 0;
}

size_t ft_rx_prefix_size(void)
{
	return (fi->rx_attr->mode & FI_MSG_PREFIX) ?
		fi->ep_attr->msg_prefix_size : 0;
}

int ft_check_opts(uint64_t flags)
{
	return (opts.options & flags) == flags;
}

static void ft_cq_set_wait_attr(void)
{
	switch (opts.comp_method) {
	case FT_COMP_SREAD:
		cq_attr.wait_obj = FI_WAIT_UNSPEC;
		cq_attr.wait_cond = FI_CQ_COND_NONE;
		break;
	case FT_COMP_WAITSET:
		assert(waitset);
		cq_attr.wait_obj = FI_WAIT_SET;
		cq_attr.wait_cond = FI_CQ_COND_NONE;
		cq_attr.wait_set = waitset;
		break;
	case FT_COMP_WAIT_FD:
		cq_attr.wait_obj = FI_WAIT_FD;
		cq_attr.wait_cond = FI_CQ_COND_NONE;
		break;
	case FT_COMP_YIELD:
		cq_attr.wait_obj = FI_WAIT_YIELD;
		cq_attr.wait_cond = FI_CQ_COND_NONE;
		break;
	default:
		cq_attr.wait_obj = FI_WAIT_NONE;
		break;
	}
}

static void ft_cntr_set_wait_attr(void)
{
	switch (opts.comp_method) {
	case FT_COMP_SREAD:
		cntr_attr.wait_obj = FI_WAIT_UNSPEC;
		break;
	case FT_COMP_WAITSET:
		assert(waitset);
		cntr_attr.wait_obj = FI_WAIT_SET;
		break;
	case FT_COMP_WAIT_FD:
		cntr_attr.wait_obj = FI_WAIT_FD;
		break;
	case FT_COMP_YIELD:
		cntr_attr.wait_obj = FI_WAIT_YIELD;
		break;
	default:
		cntr_attr.wait_obj = FI_WAIT_NONE;
		break;
	}
}

static inline int ft_rma_read_target_allowed(uint64_t caps)
{
	if (caps & (FI_RMA | FI_ATOMIC)) {
		if (caps & FI_REMOTE_READ)
			return 1;
		return !(caps & (FI_READ | FI_WRITE | FI_REMOTE_WRITE));
	}
	return 0;
}

static inline int ft_rma_write_target_allowed(uint64_t caps)
{
	if (caps & (FI_RMA | FI_ATOMIC)) {
		if (caps & FI_REMOTE_WRITE)
			return 1;
		return !(caps & (FI_READ | FI_WRITE | FI_REMOTE_WRITE));
	}
	return 0;
}

static inline int ft_check_mr_local_flag(struct fi_info *info)
{
	return ((info->mode & FI_LOCAL_MR) ||
		(info->domain_attr->mr_mode & FI_MR_LOCAL));
}

uint64_t ft_info_to_mr_access(struct fi_info *info)
{
	uint64_t mr_access = 0;
	if (ft_check_mr_local_flag(info)) {
		if (info->caps & (FI_MSG | FI_TAGGED)) {
			if (info->caps & FT_MSG_MR_ACCESS) {
				mr_access |= info->caps & FT_MSG_MR_ACCESS;
			} else {
				mr_access |= FT_MSG_MR_ACCESS;
			}
		}

		if (info->caps & (FI_RMA | FI_ATOMIC)) {
			if (info->caps & FT_RMA_MR_ACCESS) {
				mr_access |= info->caps & FT_RMA_MR_ACCESS;
			} else	{
				mr_access |= FT_RMA_MR_ACCESS;
			}
		}
	} else {
		if (info->caps & (FI_RMA | FI_ATOMIC)) {
			if (ft_rma_read_target_allowed(info->caps)) {
				mr_access |= FI_REMOTE_READ;
			}
			if (ft_rma_write_target_allowed(info->caps)) {
				mr_access |= FI_REMOTE_WRITE;
			}
		}
	}
	return mr_access;
}

void ft_fill_mr_attr(struct iovec *iov, struct fi_mr_dmabuf *dmabuf,
		     int iov_count, uint64_t access,
		     uint64_t key, enum fi_hmem_iface iface, uint64_t device,
		     struct fi_mr_attr *attr, uint64_t flags)
{
	if (flags & FI_MR_DMABUF)
		attr->dmabuf = dmabuf;
	else
		attr->mr_iov = iov;
	attr->iov_count = iov_count;
	attr->access = access;
	attr->offset = 0;
	attr->requested_key = key;
	attr->context = NULL;
	attr->iface = iface;

	switch (iface) {
	case FI_HMEM_NEURON:
		attr->device.neuron = device;
		break;
	case FI_HMEM_ZE:
		attr->device.ze = fi_hmem_ze_device(0, device);
		break;
	case FI_HMEM_CUDA:
		attr->device.cuda = device;
		break;
	default:
		break;
	}
}

bool ft_need_mr_reg(struct fi_info *fi)
{
	return (fi->caps & (FI_RMA | FI_ATOMIC)) ||
	       (fi->domain_attr->mr_mode & FI_MR_LOCAL) ||
	       ((fi->domain_attr->mr_mode & FI_MR_HMEM) &&
		(opts.options & FT_OPT_USE_DEVICE));
}

int ft_reg_mr(struct fi_info *fi, void *buf, size_t size, uint64_t access,
	      uint64_t key, enum fi_hmem_iface iface, uint64_t device,
	      struct fid_mr **mr, void **desc)
{
	struct fi_mr_attr attr = {0};
	struct iovec iov = {0};
	int ret;
	uint64_t flags;
	int dmabuf_fd;
	uint64_t dmabuf_offset;
	struct fi_mr_dmabuf dmabuf = {0};

	if (!ft_need_mr_reg(fi))
		return 0;

	iov.iov_base = buf;
	iov.iov_len = size;

	flags = (iface) ? FI_HMEM_DEVICE_ONLY : 0;

	if (opts.options & FT_OPT_REG_DMABUF_MR) {
		ret = ft_hmem_get_dmabuf_fd(iface, buf, size,
					    &dmabuf_fd, &dmabuf_offset);
		if (ret)
			return ret;

		dmabuf.fd = dmabuf_fd;
		dmabuf.offset = dmabuf_offset;
		dmabuf.len = size;
		dmabuf.base_addr = (void *)((uintptr_t) buf - dmabuf_offset);
		flags |= FI_MR_DMABUF;
	}

	ft_fill_mr_attr(&iov, &dmabuf, 1, access, key, iface, device, &attr, flags);
	ret = fi_mr_regattr(domain, &attr, flags, mr);
	if (ret)
		return ret;

	if (desc)
		*desc = fi_mr_desc(*mr);

  if (fi->domain_attr->mr_mode & FI_MR_ENDPOINT) {
		ret = fi_mr_bind(*mr, &ep->fid, 0);
		if (ret)
			return ret;

		ret = fi_mr_enable(*mr);
		if (ret)
			return ret;
	}

	return FI_SUCCESS;
}

static int ft_alloc_ctx_array(struct ft_context **mr_array, char ***mr_bufs,
			      char *default_buf, size_t mr_size,
			      uint64_t start_key)
{
	int i, ret;
	uint64_t access = ft_info_to_mr_access(fi);
	struct ft_context *context;

	*mr_array = calloc(opts.window_size, sizeof(**mr_array));
	if (!*mr_array)
		return -FI_ENOMEM;

	if (opts.options & FT_OPT_ALLOC_MULT_MR) {
		*mr_bufs = calloc(opts.window_size, sizeof(**mr_bufs));
		if (!*mr_bufs)
			return -FI_ENOMEM;
	}

	for (i = 0; i < opts.window_size; i++) {
		context = &(*mr_array)[i];
		if (!(opts.options & FT_OPT_ALLOC_MULT_MR)) {
			context->buf = default_buf + mr_size * i;
			context->mr = mr;
			context->desc = mr_desc;
			continue;
		}
		ret = ft_hmem_alloc(opts.iface, opts.device,
				    (void **) &((*mr_bufs)[i]), mr_size);
		if (ret)
			return ret;

		context->buf = (*mr_bufs)[i];

		ret = ft_reg_mr(fi, context->buf, mr_size, access,
				start_key + i, opts.iface, opts.device,
				&context->mr, &context->desc);
		if (ret)
			return ret;
	}

	return 0;
}

static void ft_set_tx_rx_sizes(size_t *set_tx, size_t *set_rx)
{
	*set_tx = opts.options & FT_OPT_SIZE ?
		  opts.transfer_size : test_size[TEST_CNT - 1].size;
	if (*set_tx > fi->ep_attr->max_msg_size)
		*set_tx = fi->ep_attr->max_msg_size;
	*set_rx = *set_tx + ft_rx_prefix_size();
	*set_tx += ft_tx_prefix_size();
}

void ft_free_host_tx_buf(void)
{
	int ret;

	ret = ft_hmem_free_host(opts.iface, dev_host_buf);
	if (ret)
		FT_PRINTERR("ft_hmem_free_host", ret);
	dev_host_buf = NULL;
}

/*
 * Include FI_MSG_PREFIX space in the allocated buffer, and ensure that the
 * buffer is large enough for a control message used to exchange addressing
 * data.
 */
int ft_alloc_msgs(void)
{
	int ret;
	int rma_resv_bytes;
	long alignment = 64;
	size_t max_msg_size;

	if (buf)
		return 0;

	if (ft_check_opts(FT_OPT_SKIP_MSG_ALLOC))
		return 0;

	if (opts.options & FT_OPT_ALLOC_MULT_MR) {
		ft_set_tx_rx_sizes(&tx_mr_size, &rx_mr_size);
		rx_size = FT_MAX_CTRL_MSG + ft_rx_prefix_size();
		tx_size = FT_MAX_CTRL_MSG + ft_tx_prefix_size();
		rx_buf_size = rx_size;
		tx_buf_size = tx_size;
	} else {
		ft_set_tx_rx_sizes(&tx_size, &rx_size);
		printf("tx_size:%lu, rx_size:%lu\n", tx_size, rx_size);
		tx_mr_size = 0;
		rx_mr_size = 0;
		rx_buf_size = MAX(rx_size, FT_MAX_CTRL_MSG) * opts.window_size;
		tx_buf_size = MAX(tx_size, FT_MAX_CTRL_MSG) * opts.window_size;
	}

	/* Allow enough space for RMA to operate in a distinct memory
	 * region that ft_sync() won't touch.
	 */
	rma_resv_bytes = FT_RMA_SYNC_MSG_BYTES +
			 MAX( ft_tx_prefix_size(), ft_rx_prefix_size() );
	printf("rma_resv_bytes:%d\n", rma_resv_bytes);
	tx_buf_size += rma_resv_bytes;
	rx_buf_size += rma_resv_bytes;

	if (opts.options & FT_OPT_ALIGN && !(opts.options & FT_OPT_USE_DEVICE)) {
		alignment = sysconf(_SC_PAGESIZE);
		if (alignment < 0)
			return -errno;
	}

	rx_buf_size = ft_get_aligned_size(rx_buf_size, alignment);
	tx_buf_size = ft_get_aligned_size(tx_buf_size, alignment);

	buf_size = rx_buf_size + tx_buf_size;
	if (opts.options & FT_OPT_ALIGN && !(opts.options & FT_OPT_USE_DEVICE)) {
		ret = posix_memalign((void **) &buf, (size_t) alignment,
				buf_size);
		if (ret) {
			FT_PRINTERR("posix_memalign", ret);
			return ret;
		}
	} else {
		/* allocate extra "alignment" bytes, to handle the case
		 * "buf" returned by ft_hmem_alloc() is not aligned.
		 */
		buf_size += alignment;
		ret = ft_hmem_alloc(opts.iface, opts.device, (void **) &buf,
				    buf_size);
		if (ret)
			return ret;

		max_msg_size = (opts.options & FT_OPT_ALLOC_MULT_MR)
				? tx_mr_size : tx_size;

		/* dev_host_buf is used by ft_fill_buf() and ft_check_buf() as
		 * staging area to copy data to and from device buffer during
		 * data setup and verification.
		 *
		 * its size therefore should be the maximum size that
		 * fi_fill_buf() and ft_check_buf() are called with, which is
		 * max_msg_size * opts.window_size, because tests like
		 * fi_rma_bw initializes all data in a window before
		 * a window started, and check all data in a window after
		 * a window completed.
		 */
		ret = ft_hmem_alloc_host(opts.iface, &dev_host_buf,
					 max_msg_size * opts.window_size);
		if (ret)
			return ret;
	}

	ret = ft_hmem_memset(opts.iface, opts.device, (void *) buf, 0, buf_size);
	if (ret)
		return ret;

	rx_buf = (char *)ft_get_aligned_addr(buf, alignment);
	tx_buf = rx_buf + rx_buf_size;
	remote_cq_data = ft_init_cq_data(fi);

	mr = &no_mr;
	if (!ft_mr_alloc_func && !ft_check_opts(FT_OPT_SKIP_REG_MR)) {
		ret = ft_reg_mr(fi, rx_buf, rx_buf_size + tx_buf_size,
				ft_info_to_mr_access(fi),
				FT_MR_KEY, opts.iface, opts.device, &mr,
				&mr_desc);
		if (ret)
			return ret;
	} else {
		if (ft_mr_alloc_func) {
			assert(!ft_check_opts(FT_OPT_SKIP_REG_MR));
			ret = ft_mr_alloc_func();
			if (ret)
				return ret;
		}
	}

	ret = ft_alloc_ctx_array(&tx_ctx_arr, &tx_mr_bufs, tx_buf,
				 tx_mr_size, FT_TX_MR_KEY);
	if (ret)
		return -FI_ENOMEM;

	ret = ft_alloc_ctx_array(&rx_ctx_arr, &rx_mr_bufs, rx_buf,
				 rx_mr_size, FT_RX_MR_KEY);
	if (ret)
		return -FI_ENOMEM;

	return 0;
}

int ft_open_domain_res(void)
{
	int ret;

	ret = fi_domain(fabric, fi, &domain, NULL);
	if (ret) {
		FT_PRINTERR("fi_domain", ret);
		return ret;
	}

	if (opts.options & FT_OPT_DOMAIN_EQ) {
		printf("Using domain event queue\n");
		ret = fi_domain_bind(domain, &eq->fid, 0);
		if (ret) {
			FT_PRINTERR("fi_domain_bind", ret);
			return ret;
		}
	}

	if (opts.options & FT_OPT_STX) {
		printf("Using shared transmit context\n");
		ret = fi_stx_context(domain, fi->tx_attr, &stx, NULL);
		if (ret) {
			FT_PRINTERR("fi_stx_context", ret);
			return ret;
		}
	}

	if (opts.options & FT_OPT_SRX) {
		printf("Using shared receive context\n");
		ret = fi_srx_context(domain, fi->rx_attr, &srx, NULL);
		if (ret) {
			FT_PRINTERR("fi_srx_context", ret);
			return ret;
		}
	}
	return 0;
}

int ft_open_fabric_res(void)
{
	int ret;

	ret = fi_fabric(fi->fabric_attr, &fabric, NULL);
	if (ret) {
		FT_PRINTERR("fi_fabric", ret);
		return ret;
	}

	ret = fi_eq_open(fabric, &eq_attr, &eq, NULL);
	if (ret) {
		FT_PRINTERR("fi_eq_open", ret);
		return ret;
	}

	return ft_open_domain_res();
}

struct fi_av_attr av_attr = {
	.type = FI_AV_MAP,
	.count = 1
};
struct fi_eq_attr eq_attr = {
	.wait_obj = FI_WAIT_UNSPEC
};
struct fi_cq_attr cq_attr = {
	.wait_obj = FI_WAIT_NONE
};
struct fi_cntr_attr cntr_attr = {
	.events = FI_CNTR_EVENTS_COMP,
	.wait_obj = FI_WAIT_NONE
};

int ft_alloc_ep_res(struct fi_info *fi, struct fid_cq **new_txcq,
		    struct fid_cq **new_rxcq, struct fid_cntr **new_txcntr,
		    struct fid_cntr **new_rxcntr,
		    struct fid_cntr **new_rma_cntr,
		    struct fid_av **new_av)
{
	int ret;

	if (cq_attr.format == FI_CQ_FORMAT_UNSPEC) {
		if (fi->caps & FI_TAGGED)
			cq_attr.format = FI_CQ_FORMAT_TAGGED;
		else
			cq_attr.format = FI_CQ_FORMAT_CONTEXT;
	}

	if (opts.options & FT_OPT_CQ_SHARED) {
		printf("Using shared completion queue\n");
		ft_cq_set_wait_attr();
		cq_attr.size = 0;

		if (opts.tx_cq_size)
			cq_attr.size += opts.tx_cq_size;
		else
			cq_attr.size += fi->tx_attr->size;

		if (opts.rx_cq_size)
			cq_attr.size += opts.rx_cq_size;
		else
			cq_attr.size += fi->rx_attr->size;

		ret = fi_cq_open(domain, &cq_attr, new_txcq, new_txcq);
		if (ret) {
			FT_PRINTERR("fi_cq_open", ret);
			return ret;
		}
		*new_rxcq = *new_txcq;
	}

	if (!(opts.options & FT_OPT_CQ_SHARED)) {
		printf("Not using shared completion queue\n");
		ft_cq_set_wait_attr();
		if (opts.tx_cq_size)
			cq_attr.size = opts.tx_cq_size;
		else
			cq_attr.size = fi->tx_attr->size;

		ret = fi_cq_open(domain, &cq_attr, new_txcq, new_txcq);
		if (ret) {
			FT_PRINTERR("fi_cq_open", ret);
			return ret;
		}
	}

	if (!(opts.options & FT_OPT_CQ_SHARED)) {
		ft_cq_set_wait_attr();
		if (opts.rx_cq_size)
			cq_attr.size = opts.rx_cq_size;
		else
			cq_attr.size = fi->rx_attr->size;

		ret = fi_cq_open(domain, &cq_attr, new_rxcq, new_rxcq);
		if (ret) {
			FT_PRINTERR("fi_cq_open", ret);
			return ret;
		}
	}

	if (!*new_av && (fi->ep_attr->type == FI_EP_RDM || fi->ep_attr->type == FI_EP_DGRAM)) {
		printf("type:%d\n", fi->ep_attr->type);
		if (fi->domain_attr->av_type != FI_AV_UNSPEC)
			av_attr.type = fi->domain_attr->av_type;

		if (opts.av_name) {
			av_attr.name = opts.av_name;
		}
		av_attr.count = opts.av_size;
		ret = fi_av_open(domain, &av_attr, new_av, NULL);
		if (ret) {
			FT_PRINTERR("fi_av_open", ret);
			return ret;
		}
	}
	return 0;
}

int ft_alloc_active_res(struct fi_info *fi)
{
	int ret;
	ret = ft_alloc_ep_res(fi, &txcq, &rxcq, &txcntr, &rxcntr, &rma_cntr, &av);
	if (ret)
		return ret;

	ret = fi_endpoint(domain, fi, &ep, NULL);
	if (ret) {
		FT_PRINTERR("fi_endpoint", ret);
		return ret;
	}

	return 0;
}

int ft_init(void)
{
	int ret;

	tx_seq = 0;
	rx_seq = 0;
	tx_cq_cntr = 0;
	rx_cq_cntr = 0;

	ret = ft_startup();
	if (ret) {
		FT_ERR("ft_startup: %d", ret);
		return ret;
	}

	ret = ft_hmem_init(opts.iface);
	if (ret)
		FT_PRINTERR("ft_hmem_init", ret);
	return ret;
}

int ft_getinfo(struct fi_info *hints, struct fi_info **info)
{
	char *node, *service;
	uint64_t flags = 0;
	int ret;

	ret = ft_read_addr_opts(&node, &service, hints, &flags, &opts);
	if (ret)
		return ret;

	if (!hints->ep_attr->type)
		hints->ep_attr->type = FI_EP_RDM;

	if (opts.options & FT_OPT_ENABLE_HMEM) {
		printf("enable hem\n");
		hints->caps |= FI_HMEM;
		hints->domain_attr->mr_mode |= FI_MR_HMEM;
	}

	ret = fi_getinfo(FT_FIVERSION, node, service, flags, hints, info);
	if (ret) {
		FT_PRINTERR("fi_getinfo", ret);
		return ret;
	}

	if (!ft_check_prefix_forced(*info, &opts)) {
		FT_ERR("Provider disabled requested prefix mode.");
		return -FI_ENODATA;
	}

	return 0;
}

int ft_start_server(void)
{
	int ret;

	ret = ft_init();
	if (ret)
		return ret;

	ret = ft_getinfo(hints, &fi_pep);
	if (ret)
		return ret;

	ret = fi_fabric(fi_pep->fabric_attr, &fabric, NULL);
	if (ret) {
		FT_PRINTERR("fi_fabric", ret);
		return ret;
	}

	ret = fi_eq_open(fabric, &eq_attr, &eq, NULL);
	if (ret) {
		FT_PRINTERR("fi_eq_open", ret);
		return ret;
	}

	ret = fi_passive_ep(fabric, fi_pep, &pep, NULL);
	if (ret) {
		FT_PRINTERR("fi_passive_ep", ret);
		return ret;
	}

	ret = fi_pep_bind(pep, &eq->fid, 0);
	if (ret) {
		FT_PRINTERR("fi_pep_bind", ret);
		return ret;
	}

	ret = fi_listen(pep);
	if (ret) {
		FT_PRINTERR("fi_listen", ret);
		return ret;
	}

	return 0;
}

int ft_complete_connect(struct fid_ep *ep, struct fid_eq *eq)
{
	struct fi_eq_cm_entry entry;
	uint32_t event;
	ssize_t rd;
	int ret;

	rd = fi_eq_sread(eq, &event, &entry, sizeof(entry), -1, 0);
	if (rd != sizeof(entry)) {
		FT_PROCESS_EQ_ERR(rd, eq, "fi_eq_sread", "accept");
		ret = (int) rd;
		return ret;
	}

	if (event != FI_CONNECTED || entry.fid != &ep->fid) {
		fprintf(stderr, "Unexpected CM event %d fid %p (ep %p)\n",
			event, entry.fid, ep);
		ret = -FI_EOTHER;
		return ret;
	}

	return 0;
}

int ft_verify_info(struct fi_info *fi_pep, struct fi_info *info)
{
	if (!info || !info->fabric_attr || !info->domain_attr ||
	    !info->ep_attr || !info->tx_attr || !info->rx_attr)
		return -FI_EINVAL;

	if (!info->fabric_attr->prov_name ||
	    !info->fabric_attr->name || !info->domain_attr->name ||
	    info->fabric_attr->api_version != fi_pep->fabric_attr->api_version)
		return -FI_EINVAL;

	return 0;
}

int ft_retrieve_conn_req(struct fid_eq *eq, struct fi_info **fi)
{
	struct fi_eq_cm_entry entry;
	uint32_t event;
	ssize_t rd;
	int ret;

	rd = fi_eq_sread(eq, &event, &entry, sizeof(entry), -1, 0);
	if (rd != sizeof entry) {
		FT_PROCESS_EQ_ERR(rd, eq, "fi_eq_sread", "listen");
		return (int) rd;
	}

	*fi = entry.info;
	if (event != FI_CONNREQ) {
		fprintf(stderr, "Unexpected CM event %d\n", event);
		ret = -FI_EOTHER;
		return ret;
	}

	if ((ret = ft_verify_info(fi_pep, entry.info))) {
		printf("ret: %d\n", ret);
		return ret;
	}

	return 0;
}

int ft_accept_connection(struct fid_ep *ep, struct fid_eq *eq)
{
	int ret;

	ret = fi_accept(ep, NULL, 0);
	if (ret) {
		FT_PRINTERR("fi_accept", ret);
		return ret;
	}

	ret = ft_complete_connect(ep, eq);
	if (ret)
		return ret;

	return 0;
}

int ft_server_connect(void)
{
	int ret;

	ret = ft_retrieve_conn_req(eq, &fi);
	if (ret)
		goto err;

	ret = ft_open_domain_res();
	if (ret)
		goto err;

	ret = ft_alloc_active_res(fi);
	if (ret)
		goto err;

	ret = ft_enable_ep_recv();
	if (ret)
		goto err;

	ret = ft_accept_connection(ep, eq);
	if (ret)
		goto err;

	return 0;

err:
	if (fi)
		fi_reject(pep, fi->handle, NULL, 0);
	return ret;
}

int ft_connect_ep(struct fid_ep *ep,
		struct fid_eq *eq, fi_addr_t *remote_addr)
{
	int ret;

	ret = fi_connect(ep, remote_addr, NULL, 0);
	if (ret) {
		FT_PRINTERR("fi_connect", ret);
		return ret;
	}

	ret = ft_complete_connect(ep, eq);
	if (ret)
		return ret;

	return 0;
}

int ft_client_connect(void)
{
	int ret;

	ret = ft_init();
	if (ret)
		return ret;

	ret = ft_getinfo(hints, &fi);
	if (ret)
		return ret;

	ret = ft_open_fabric_res();
	if (ret)
		return ret;

	ret = ft_alloc_active_res(fi);
	if (ret)
		return ret;

	ret = ft_enable_ep_recv();
	if (ret)
		return ret;

	ret = ft_connect_ep(ep, eq, fi->dest_addr);
	if (ret)
		return ret;

	return 0;
}

int ft_init_fabric(void)
{
	int ret;

	ret = ft_init();
	if (ret)
		return ret;

	printf("ft_getinfo\n");
	ret = ft_getinfo(hints, &fi);
	if (ret)
		return ret;

	printf("ft_open_fabric_res\n");
	ret = ft_open_fabric_res();
	if (ret)
		return ret;

	printf("ft_alloc_active_res\n");
	ret = ft_alloc_active_res(fi);
	if (ret)
		return ret;

	printf("ft_enable_ep_recv\n");
	ret = ft_enable_ep_recv();
	if (ret)
		return ret;

	printf("ft_init_av\n");
	ret = ft_init_av();
	if (ret)
		return ret;

	return 0;
}

int ft_get_cq_fd(struct fid_cq *cq, int *fd)
{
	int ret = FI_SUCCESS;

	if (cq && opts.comp_method == FT_COMP_WAIT_FD) {
		printf("FT_COMP_WAIT_FD\n");
		ret = fi_control(&cq->fid, FI_GETWAIT, fd);
		if (ret)
			FT_PRINTERR("fi_control(FI_GETWAIT)", ret);
	}

	return ret;
}

int ft_enable_ep(struct fid_ep *bind_ep, struct fid_eq *bind_eq, struct fid_av *bind_av,
		 struct fid_cq *bind_txcq, struct fid_cq *bind_rxcq,
		 struct fid_cntr *bind_txcntr, struct fid_cntr *bind_rxcntr,
		 struct fid_cntr *bind_rma_cntr)
{
	uint64_t flags;
	int ret;

	if ((fi->ep_attr->type == FI_EP_MSG || fi->caps & FI_MULTICAST ||
	    fi->caps & FI_COLLECTIVE) && !(opts.options & FT_OPT_DOMAIN_EQ)) {
		FT_EP_BIND(bind_ep, bind_eq, 0);
	}

	FT_EP_BIND(bind_ep, bind_av, 0);
	FT_EP_BIND(bind_ep, stx, 0);
	FT_EP_BIND(bind_ep, srx, 0);

	flags = FI_TRANSMIT;
	if (!(opts.options & FT_OPT_TX_CQ))
		flags |= FI_SELECTIVE_COMPLETION;
	FT_EP_BIND(bind_ep, bind_txcq, flags);

	flags = FI_RECV;
	if (!(opts.options & FT_OPT_RX_CQ))
		flags |= FI_SELECTIVE_COMPLETION;
	FT_EP_BIND(bind_ep, bind_rxcq, flags);

	ret = ft_get_cq_fd(bind_txcq, &tx_fd);
	if (ret)
		return ret;

	ret = ft_get_cq_fd(bind_rxcq, &rx_fd);
	if (ret)
		return ret;

	/* TODO: use control structure to select counter bindings explicitly */
	if (opts.options & FT_OPT_TX_CQ)
		flags = 0;
	else
		flags = FI_SEND;

	if (hints->caps & (FI_RMA | FI_ATOMICS))
		flags |= FI_WRITE | FI_READ;
	FT_EP_BIND(bind_ep, bind_txcntr, flags);

	if (opts.options & FT_OPT_RX_CQ)
		flags = 0;
	else
		flags = FI_RECV;

	FT_EP_BIND(bind_ep, bind_rxcntr, flags);

	if (hints->caps & (FI_RMA | FI_ATOMICS) && hints->caps & FI_RMA_EVENT) {
		flags = fi->caps & (FI_REMOTE_WRITE | FI_REMOTE_READ);
		FT_EP_BIND(bind_ep, bind_rma_cntr, flags);
	}

	ret = fi_enable(bind_ep);
	if (ret) {
		FT_PRINTERR("fi_enable", ret);
		return ret;
	}

	return 0;
}

int ft_enable_ep_recv(void)
{
	int ret;

	ret = ft_enable_ep(ep, eq, av, txcq, rxcq, txcntr, rxcntr, rma_cntr);
	if (ret)
		return ret;

	ret = ft_alloc_msgs();
	if (ret)
		return ret;

	if (!ft_check_opts(FT_OPT_SKIP_MSG_ALLOC) &&
	    (fi->caps & (FI_MSG | FI_TAGGED))) {
		/* Initial receive will get remote address for unconnected EPs */
		ret = ft_post_rx(ep, MAX(rx_size, FT_MAX_CTRL_MSG), &rx_ctx);
		if (ret)
			return ret;
	}

	return 0;
}

int ft_av_insert(struct fid_av *av, void *addr, size_t count, fi_addr_t *fi_addr,
		uint64_t flags, void *context)
{
	int ret;

	ret = fi_av_insert(av, addr, count, fi_addr, flags, context);
	if (ret < 0) {
		FT_PRINTERR("fi_av_insert", ret);
		return ret;
	} else if (ret != count) {
		FT_ERR("fi_av_insert: number of addresses inserted = %d;"
			       " number of addresses given = %zd\n", ret, count);
		return -EXIT_FAILURE;
	}

	return 0;
}

int ft_init_av(void)
{
	return ft_init_av_dst_addr(av, ep, &remote_fi_addr);
}

/* TODO: retry send for unreliable endpoints */
int ft_init_av_dst_addr(struct fid_av *av_ptr, struct fid_ep *ep_ptr,
		fi_addr_t *remote_addr)
{
	char temp[FT_MAX_CTRL_MSG];
	size_t addrlen;
	int ret;

	if (opts.options & FT_OPT_SKIP_ADDR_EXCH)
		return 0;

	if (opts.dst_addr) {
		ret = ft_av_insert(av_ptr, fi->dest_addr, 1, remote_addr, 0, NULL);
		if (ret)
			return ret;

		addrlen = FT_MAX_CTRL_MSG;
		ret = fi_getname(&ep_ptr->fid, temp, &addrlen);
		if (ret) {
			FT_PRINTERR("fi_getname", ret);
			return ret;
		}

		ret = ft_hmem_copy_to(opts.iface, opts.device,
				      tx_buf + ft_tx_prefix_size(), temp, addrlen);
		if (ret)
			return ret;

		ret = (int) ft_tx(ep, *remote_addr, addrlen, &tx_ctx);
		if (ret)
			return ret;

		ret = ft_rx(ep, 1);
		if (ret)
			return ret;
	} else {
		ret = ft_get_rx_comp(rx_seq);
		if (ret)
			return ret;

		ret = ft_hmem_copy_from(opts.iface, opts.device, temp,
					rx_buf + ft_rx_prefix_size(),
					FT_MAX_CTRL_MSG);
		if (ret)
			return ret;

		/* Test passing NULL fi_addr on one of the sides (server) if
		 * AV type is FI_AV_TABLE */
		ret = ft_av_insert(av_ptr, temp, 1,
				   ((fi->domain_attr->av_type == FI_AV_TABLE) ?
				       NULL : remote_addr), 0, NULL);
		if (ret)
			return ret;

		ret = ft_post_rx(ep, rx_size, &rx_ctx);
		if (ret)
			return ret;

		if (fi->domain_attr->av_type == FI_AV_TABLE)
			*remote_addr = 0;

		ret = (int) ft_tx(ep, *remote_addr, 1, &tx_ctx);
		if (ret)
			return ret;
	}

set_rx_seq_close:
	/*
	* For a test which does not have MSG or TAGGED
	* capabilities, but has RMA/Atomics and uses the OOB sync.
	* If no recv is going to be posted,
	* then the rx_seq needs to be incremented to wait on the first RMA/Atomic
	* completion.
	*/
	if (!(fi->caps & FI_MSG) && !(fi->caps & FI_TAGGED) && opts.oob_port)
		rx_seq++;

	return 0;
}

int ft_exchange_keys(struct fi_rma_iov *peer_iov)
{
	char temp[FT_MAX_CTRL_MSG];
	struct fi_rma_iov *rma_iov = (struct fi_rma_iov *) temp;
	size_t key_size = 0, len;
	uint64_t addr;
	int ret;

	if (fi->domain_attr->mr_mode & FI_MR_RAW) {
		ret = fi_mr_raw_attr(mr, &addr, NULL, &key_size, 0);
		if (ret != -FI_ETOOSMALL)
			return ret;
		len = sizeof(*rma_iov) + key_size - sizeof(rma_iov->key);
		if (len > FT_MAX_CTRL_MSG) {
			FT_PRINTERR("Raw key too large for ctrl message",
				    -FI_ETOOSMALL);
			return -FI_ETOOSMALL;
		}
	} else {
		len = sizeof(*rma_iov);
	}

	if ((fi->domain_attr->mr_mode == FI_MR_BASIC) ||
	    (fi->domain_attr->mr_mode & FI_MR_VIRT_ADDR)) {
		rma_iov->addr = (uintptr_t) rx_buf + ft_rx_prefix_size();
	} else {
		rma_iov->addr = 0;
	}

	if (fi->domain_attr->mr_mode & FI_MR_RAW) {
		ret = fi_mr_raw_attr(mr, &addr, (uint8_t *) &rma_iov->key,
				     &key_size, 0);
		if (ret)
			return ret;
	} else {
		rma_iov->key = fi_mr_key(mr);
	}

	ret = ft_hmem_copy_to(opts.iface, opts.device,
			      tx_buf + ft_tx_prefix_size(), temp, len);
	if (ret)
		return ret;

	ret = ft_tx(ep, remote_fi_addr, len + ft_tx_prefix_size(), &tx_ctx);
	if (ret)
		return ret;

	ret = ft_get_rx_comp(rx_seq);
	if (ret)
		return ret;

	ret = ft_hmem_copy_from(opts.iface, opts.device, temp,
				rx_buf + ft_rx_prefix_size(), FT_MAX_CTRL_MSG);
	if (ret)
		return ret;

	if (fi->domain_attr->mr_mode & FI_MR_RAW) {
		peer_iov->addr = rma_iov->addr;
		peer_iov->len = rma_iov->len;
		ret = fi_mr_map_raw(domain, rma_iov->addr,
				    (uint8_t *) &rma_iov->key, key_size,
				    &peer_iov->key, 0);
		if (ret)
			return ret;
	} else {
		*peer_iov = *rma_iov;
	}

	ret = ft_post_rx(ep, rx_size, &rx_ctx);
	if (ret)
		return ret;

	return ft_sync();
}

static void ft_cleanup_mr_array(struct ft_context *ctx_arr, char **mr_bufs)
{
	int i, ret;

	if (!mr_bufs)
		return;

	for (i = 0; i < opts.window_size; i++) {
		FT_CLOSE_FID(ctx_arr[i].mr);
		ret = ft_hmem_free(opts.iface, mr_bufs[i]);
		if (ret)
			FT_PRINTERR("ft_hmem_free", ret);
	}
}

void ft_close_fids(void)
{
	FT_CLOSE_FID(mc);
	FT_CLOSE_FID(alias_ep);
	if (fi && fi->domain_attr->mr_mode & FI_MR_ENDPOINT) {
		if (mr != &no_mr) {
			FT_CLOSE_FID(mr);
			mr = &no_mr;
		}
	}
	FT_CLOSE_FID(ep);
	FT_CLOSE_FID(pep);
	if (opts.options & FT_OPT_CQ_SHARED) {
		FT_CLOSE_FID(txcq);
	} else {
		FT_CLOSE_FID(rxcq);
		FT_CLOSE_FID(txcq);
	}
	FT_CLOSE_FID(rxcntr);
	FT_CLOSE_FID(txcntr);
	FT_CLOSE_FID(rma_cntr);
	FT_CLOSE_FID(pollset);
	if (mr != &no_mr)
		FT_CLOSE_FID(mr);
	FT_CLOSE_FID(av);
	FT_CLOSE_FID(srx);
	FT_CLOSE_FID(stx);
	FT_CLOSE_FID(domain);
	FT_CLOSE_FID(eq);
	FT_CLOSE_FID(waitset);
	FT_CLOSE_FID(fabric);
}

/* We need to free any data that we allocated before freeing the
 * hints.  Windows doesn't like it when a library frees memory that
 * was allocated by the application.
 */
void ft_freehints(struct fi_info *hints)
{
	if (!hints)
		return;

	if (hints->domain_attr->name) {
		free(hints->domain_attr->name);
		hints->domain_attr->name = NULL;
	}
	if (hints->fabric_attr->name) {
		free(hints->fabric_attr->name);
		hints->fabric_attr->name = NULL;
	}
	if (hints->fabric_attr->prov_name) {
		free(hints->fabric_attr->prov_name);
		hints->fabric_attr->prov_name = NULL;
	}
	if (hints->src_addr) {
		free(hints->src_addr);
		hints->src_addr = NULL;
		hints->src_addrlen = 0;
	}
	if (hints->dest_addr) {
		free(hints->dest_addr);
		hints->dest_addr = NULL;
		hints->dest_addrlen = 0;
	}

	fi_freeinfo(hints);
}

void ft_free_res(void)
{
	int ret;

	ft_cleanup_mr_array(tx_ctx_arr, tx_mr_bufs);
	ft_cleanup_mr_array(rx_ctx_arr, rx_mr_bufs);

	free(tx_ctx_arr);
	free(rx_ctx_arr);
	tx_ctx_arr = NULL;
	rx_ctx_arr = NULL;

	ft_close_fids();
	free(user_test_sizes);
	if (buf) {
		ret = ft_hmem_free(opts.iface, buf);
		if (ret)
			FT_PRINTERR("ft_hmem_free", ret);
		buf = rx_buf = tx_buf = NULL;
		buf_size = rx_size = tx_size = tx_mr_size = rx_mr_size = 0;
	}
	if (dev_host_buf)
		ft_free_host_tx_buf();

	if (fi_pep) {
		fi_freeinfo(fi_pep);
		fi_pep = NULL;
	}
	if (fi) {
		fi_freeinfo(fi);
		fi = NULL;
	}
	if (hints) {
		ft_freehints(hints);
		hints = NULL;
	}

	ret = ft_hmem_cleanup(opts.iface);
	if (ret)
		FT_PRINTERR("ft_hmem_cleanup", ret);
}

static int dupaddr(void **dst_addr, size_t *dst_addrlen,
		void *src_addr, size_t src_addrlen)
{
	*dst_addr = malloc(src_addrlen);
	if (!*dst_addr) {
		FT_ERR("address allocation failed");
		return EAI_MEMORY;
	}
	*dst_addrlen = src_addrlen;
	memcpy(*dst_addr, src_addr, src_addrlen);
	return 0;
}

static int getaddr(char *node, char *service,
		   struct fi_info *hints, uint64_t flags)
{
	int ret;
	struct fi_info *fi;

	if (!node && !service) {
		if (flags & FI_SOURCE) {
			hints->src_addr = NULL;
			hints->src_addrlen = 0;
		} else {
			hints->dest_addr = NULL;
			hints->dest_addrlen = 0;
		}
		return 0;
	}

	ret = fi_getinfo(FT_FIVERSION, node, service, flags, hints, &fi);
	if (ret) {
		FT_PRINTERR("fi_getinfo", ret);
		return ret;
	}
	hints->addr_format = fi->addr_format;

	if (flags & FI_SOURCE) {
		ret = dupaddr(&hints->src_addr, &hints->src_addrlen,
				fi->src_addr, fi->src_addrlen);
	} else {
		ret = dupaddr(&hints->dest_addr, &hints->dest_addrlen,
				fi->dest_addr, fi->dest_addrlen);
	}

	fi_freeinfo(fi);
	return ret;
}

int ft_getsrcaddr(char *node, char *service, struct fi_info *hints)
{
	return getaddr(node, service, hints, FI_SOURCE);
}

int ft_read_addr_opts(char **node, char **service, struct fi_info *hints,
		uint64_t *flags, struct ft_opts *opts)
{
	int ret;
  if (opts->dst_addr) {
		if (!opts->dst_port)
			opts->dst_port = default_port;

		ret = ft_getsrcaddr(opts->src_addr, opts->src_port, hints);
		if (ret)
			return ret;
		*node = opts->dst_addr;
		*service = opts->dst_port;
	} else {
		if (!opts->src_port)
			opts->src_port = default_port;

		*node = opts->src_addr;
		*service = opts->src_port;
		*flags = FI_SOURCE;
	}

	return 0;
}

void init_test(struct ft_opts *opts, char *test_name, size_t test_name_len)
{
	char sstr[FT_STR_LEN];

	size_str(sstr, opts->transfer_size);
	if (!strcmp(test_name, "custom"))
		snprintf(test_name, test_name_len, "%s_lat", sstr);
	if (!(opts->options & FT_OPT_ITER))
		opts->iterations = size_to_count(opts->transfer_size);
}

void ft_force_progress(void)
{
	if (txcq)
		(void) fi_cq_read(txcq, NULL, 0);
	if (rxcq)
		(void) fi_cq_read(rxcq, NULL, 0);
}

int ft_progress(struct fid_cq *cq, uint64_t total, uint64_t *cq_cntr)
{
	struct fi_cq_err_entry comp;
	int ret;

	ret = fi_cq_read(cq, &comp, 1);
	if (ret > 0)
		(*cq_cntr)++;

	if (ret >= 0 || ret == -FI_EAGAIN)
		return 0;

	if (ret == -FI_EAVAIL) {
		ret = ft_cq_readerr(cq);
		(*cq_cntr)++;
	} else {
		FT_PRINTERR("fi_cq_read/sread", ret);
	}
	return ret;
}

#define FT_POST(post_fn, progress_fn, cq, seq, cq_cntr, op_str, ...)		\
	do {									\
		int timeout_save;						\
		int ret, rc;							\
										\
		while (1) {							\
			ret = post_fn(__VA_ARGS__);				\
			if (!ret)						\
				break;						\
										\
			if (ret != -FI_EAGAIN) {				\
				FT_PRINTERR(op_str, ret);			\
				return ret;					\
			}							\
										\
			timeout_save = timeout;					\
			timeout = 0;						\
			rc = progress_fn(cq, seq, cq_cntr);			\
			if (rc && rc != -FI_EAGAIN) {				\
				FT_ERR("Failed to get " op_str " completion");	\
				return rc;					\
			}							\
			timeout = timeout_save;					\
		}								\
		seq++;								\
	} while (0)

ssize_t ft_post_tx_buf(struct fid_ep *ep, fi_addr_t fi_addr, size_t size,
		       uint64_t data, void *ctx,
		       void *op_buf, void *op_mr_desc, uint64_t op_tag)
{
	size += ft_tx_prefix_size();
	if (hints->caps & FI_TAGGED) {
		op_tag = op_tag ? op_tag : tx_seq;
		if (data != NO_CQ_DATA) {
			FT_POST(fi_tsenddata, ft_progress, txcq, tx_seq,
				&tx_cq_cntr, "transmit", ep, op_buf, size,
				op_mr_desc, data, fi_addr, op_tag, ctx);
		} else {
			FT_POST(fi_tsend, ft_progress, txcq, tx_seq,
				&tx_cq_cntr, "transmit", ep, op_buf, size,
				op_mr_desc, fi_addr, op_tag, ctx);
		}
	} else {
		if (data != NO_CQ_DATA) {
			FT_POST(fi_senddata, ft_progress, txcq, tx_seq,
				&tx_cq_cntr, "transmit", ep, op_buf, size,
				op_mr_desc, data, fi_addr, ctx);

		} else {
			FT_POST(fi_send, ft_progress, txcq, tx_seq,
				&tx_cq_cntr, "transmit", ep, op_buf, size,
				op_mr_desc, fi_addr, ctx);
		}
	}
	return 0;
}

ssize_t ft_post_tx(struct fid_ep *ep, fi_addr_t fi_addr, size_t size,
		   uint64_t data, void *ctx)
{
	return ft_post_tx_buf(ep, fi_addr, size, data,
			      ctx, tx_buf, mr_desc, ft_tag);
}

ssize_t ft_tx(struct fid_ep *ep, fi_addr_t fi_addr, size_t size, void *ctx)
{
	ssize_t ret;

	if (ft_check_opts(FT_OPT_VERIFY_DATA | FT_OPT_ACTIVE)) {
		ret = ft_fill_buf((char *) tx_buf + ft_tx_prefix_size(), size);
		if (ret)
			return ret;
	}

	ret = ft_post_tx(ep, fi_addr, size, NO_CQ_DATA, ctx);
	if (ret)
		return ret;

	ret = ft_get_tx_comp(tx_seq);
	return ret;
}

ssize_t ft_tx_rma(enum ft_rma_opcodes rma_op, struct fi_rma_iov *remote,
		  struct fid_ep *ep, fi_addr_t fi_addr, size_t size, void *ctx)
{
	ssize_t ret;

	if (ft_check_opts(FT_OPT_VERIFY_DATA | FT_OPT_ACTIVE)) {
		/* Fill data. Last byte reserved for iteration number */
		ret = ft_fill_buf((char *) tx_buf, size - 1);
		if (ret)
			return ret;
	}

	ret = ft_post_rma(rma_op, tx_buf, size, remote, ctx);
	if (ret)
		return ret;

	ret = ft_get_tx_comp(tx_seq);
	return ret;
}

ssize_t ft_post_inject_buf(struct fid_ep *ep, fi_addr_t fi_addr, size_t size,
			   void *op_buf, uint64_t op_tag)
{
	if (hints->caps & FI_TAGGED) {
		FT_POST(fi_tinject, ft_progress, txcq, tx_seq, &tx_cq_cntr,
			"inject", ep, op_buf, size + ft_tx_prefix_size(),
			fi_addr, op_tag);
	} else {
		FT_POST(fi_inject, ft_progress, txcq, tx_seq, &tx_cq_cntr,
			"inject", ep, op_buf, size + ft_tx_prefix_size(),
			fi_addr);
	}

	tx_cq_cntr++;
	return 0;
}

ssize_t ft_post_inject(struct fid_ep *ep, fi_addr_t fi_addr, size_t size)
{
	return ft_post_inject_buf(ep, fi_addr, size, tx_buf, tx_seq);
}

ssize_t ft_inject(struct fid_ep *ep, fi_addr_t fi_addr, size_t size)
{
	ssize_t ret;

	if (ft_check_opts(FT_OPT_VERIFY_DATA | FT_OPT_ACTIVE)) {
		ret = ft_fill_buf((char *) tx_buf + ft_tx_prefix_size(), size);
		if (ret)
			return ret;
	}

	ret = ft_post_inject(ep, fi_addr, size);
	if (ret)
		return ret;

	return ret;
}

ssize_t ft_inject_rma(enum ft_rma_opcodes rma_op, struct fi_rma_iov *remote,
		      struct fid_ep *ep, fi_addr_t fi_addr, size_t size)
{
	ssize_t ret;

	if (ft_check_opts(FT_OPT_VERIFY_DATA | FT_OPT_ACTIVE)) {
		/* Fill data. Last byte reserved for iteration number */
		ret = ft_fill_buf((char *) tx_buf, size - 1);
		if (ret)
			return ret;
	}

	ret = ft_post_rma_inject(rma_op, tx_buf, size, remote);
	if (ret)
		return ret;

	return ret;
}

static size_t ft_remote_write_offset(const char *buf)
{
	assert(buf >= tx_buf && buf < (tx_buf + tx_buf_size));
	/* rx_buf area is at the beginning of the remote region */
	return buf - tx_buf;
}

static size_t ft_remote_read_offset(const char *buf)
{
	assert(buf >= rx_buf && buf < (rx_buf + rx_buf_size));
	/* We want to read from remote peer's tx_buf area,
	 * which immediately follow the rx_buf, hence add rx_buf_size
	 */
	return buf - rx_buf + rx_buf_size;
}

ssize_t ft_post_rma(enum ft_rma_opcodes op, char *buf, size_t size,
		struct fi_rma_iov *remote, void *context)
{
	switch (op) {
	case FT_RMA_WRITE:
		FT_POST(fi_write, ft_progress, txcq, tx_seq, &tx_cq_cntr,
			"fi_write", ep, buf, size, mr_desc,
			remote_fi_addr, remote->addr + ft_remote_write_offset(buf),
			remote->key, context);
		break;
	case FT_RMA_WRITEDATA:
		FT_POST(fi_writedata, ft_progress, txcq, tx_seq, &tx_cq_cntr,
			"fi_writedata", ep, buf, size, mr_desc,
			remote_cq_data, remote_fi_addr,
			remote->addr + ft_remote_write_offset(buf),
			remote->key, context);
		break;
	case FT_RMA_READ:
		FT_POST(fi_read, ft_progress, txcq, tx_seq, &tx_cq_cntr,
			"fi_read", ep, buf, size, mr_desc,
			remote_fi_addr, remote->addr + ft_remote_read_offset(buf),
			remote->key, context);
		break;
	default:
		FT_ERR("Unknown RMA op type\n");
		return EXIT_FAILURE;
	}

	return 0;
}

ssize_t ft_post_rma_inject(enum ft_rma_opcodes op, char *buf, size_t size,
		struct fi_rma_iov *remote)
{
	switch (op) {
	case FT_RMA_WRITE:
		FT_POST(fi_inject_write, ft_progress, txcq, tx_seq, &tx_cq_cntr,
			"fi_inject_write", ep, buf, opts.transfer_size,
			remote_fi_addr, remote->addr + ft_remote_write_offset(buf),
			remote->key);
		break;
	case FT_RMA_WRITEDATA:
		FT_POST(fi_inject_writedata, ft_progress, txcq, tx_seq,
			&tx_cq_cntr, "fi_inject_writedata", ep, buf,
			opts.transfer_size, remote_cq_data, remote_fi_addr,
			remote->addr + ft_remote_write_offset(buf), remote->key);
		break;
	default:
		FT_ERR("Unknown RMA inject op type\n");
		return EXIT_FAILURE;
	}

	tx_cq_cntr++;
	return 0;
}

ssize_t ft_post_rx_buf(struct fid_ep *ep, size_t size, void *ctx,
		       void *op_buf, void *op_mr_desc, uint64_t op_tag)
{
	size = MAX(size, FT_MAX_CTRL_MSG) + ft_rx_prefix_size();
	if (hints->caps & FI_TAGGED) {
		op_tag = op_tag ? op_tag : rx_seq;
		FT_POST(fi_trecv, ft_progress, rxcq, rx_seq, &rx_cq_cntr,
			"receive", ep, op_buf, size, op_mr_desc,
			remote_fi_addr, op_tag, 0, ctx);
	} else {
		FT_POST(fi_recv, ft_progress, rxcq, rx_seq, &rx_cq_cntr,
			"receive", ep, op_buf, size, op_mr_desc, remote_fi_addr, ctx);
	}
	return 0;
}

ssize_t ft_post_rx(struct fid_ep *ep, size_t size, void *ctx)
{
	return ft_post_rx_buf(ep, size, ctx, rx_buf, mr_desc, ft_tag);
}

ssize_t ft_rx(struct fid_ep *ep, size_t size)
{
	ssize_t ret;

	ret = ft_get_rx_comp(rx_seq);
	if (ret)
		return ret;

	if (ft_check_opts(FT_OPT_VERIFY_DATA | FT_OPT_ACTIVE)) {
		ret = ft_check_buf((char *) rx_buf + ft_rx_prefix_size(), size);
		if (ret)
			return ret;
	}
	/* TODO: verify CQ data, if available */

	/* Ignore the size arg. Post a buffer large enough to handle all message
	 * sizes. ft_sync() makes use of ft_rx() and gets called in tests just before
	 * message size is updated. The recvs posted are always for the next incoming
	 * message */
	ret = ft_post_rx(ep, rx_size, &rx_ctx);
	return ret;
}

ssize_t ft_rx_rma(int iter, enum ft_rma_opcodes rma_op, struct fid_ep *ep,
		  size_t size)
{
	ssize_t ret;

	switch (rma_op) {
	case FT_RMA_WRITE:
		/* No completion at target. Poll the recv buf instead. */
		ret = ft_rma_poll_buf(rx_buf, iter, size);
		if (ret)
			return ret;
		break;
	case FT_RMA_WRITEDATA:
		/* Get recv-side write-imm completion */
		ret = ft_get_rx_comp(rx_seq);
		if (ret)
			return ret;
		break;
	default:
		FT_ERR("Unsupported RMA op type");
		return EXIT_FAILURE;
	}

	if (ft_check_opts(FT_OPT_VERIFY_DATA | FT_OPT_ACTIVE)) {
		ret = ft_check_buf((char *) rx_buf, size - 1);
		if (ret)
			return ret;
	}

	/* TODO: verify CQ data, if available */

	if (rma_op == FT_RMA_WRITEDATA) {
		if (fi->rx_attr->mode & FI_RX_CQ_DATA) {
			ret = ft_post_rx(ep, 0, &rx_ctx);
		} else {
			/* Just increment the seq # instead of
			 * posting recv so that we wait for
			 * remote write completion on the next
			 * iteration */
			rx_seq++;
		}
	}

	return ret;
}

/*
 * Received messages match tagged buffers in order, but the completions can be
 * reported out of order.  A tag is valid if it's within the current window.
 */
static inline int
ft_tag_is_valid(struct fid_cq * cq, struct fi_cq_err_entry *comp, uint64_t tag)
{
	int valid = 1;

	if (opts.options & FT_OPT_DISABLE_TAG_VALIDATION)
		return valid;

	if ((hints->caps & FI_TAGGED) && (cq == rxcq)) {
		if (opts.options & FT_OPT_BW) {
			/* valid: (tag - window) < comp->tag < (tag + window) */
			valid = (tag < comp->tag + opts.window_size) &&
				(comp->tag < tag + opts.window_size);
		} else {
			valid = (comp->tag == tag);
		}

		if (!valid) {
			FT_ERR("Tag mismatch!. Expected: %"PRIu64", actual: %"
				PRIu64, tag, comp->tag);
		}
	}

	return valid;
}
/*
 * fi_cq_err_entry can be cast to any CQ entry format.
 */
static int ft_spin_for_comp(struct fid_cq *cq, uint64_t *cur,
			    uint64_t total, int timeout,
			    uint64_t tag)
{
	struct fi_cq_err_entry comp;
	struct timespec a, b;
	int ret;

	if (timeout >= 0)
		clock_gettime(CLOCK_MONOTONIC, &a);

	do {
		ret = fi_cq_read(cq, &comp, 1);
		if (ret > 0) {
			if (timeout >= 0)
				clock_gettime(CLOCK_MONOTONIC, &a);
			if (!ft_tag_is_valid(cq, &comp, tag ? tag : rx_cq_cntr))
				return -FI_EOTHER;
			(*cur)++;
		} else if (ret < 0 && ret != -FI_EAGAIN) {
			return ret;
		} else if (timeout >= 0) {
			clock_gettime(CLOCK_MONOTONIC, &b);
			if ((b.tv_sec - a.tv_sec) > timeout) {
				fprintf(stderr, "%ds timeout expired\n", timeout);
				return -FI_ENODATA;
			}
		}
	} while (total - *cur > 0);

	return 0;
}

/*
 * fi_cq_err_entry can be cast to any CQ entry format.
 */
static int ft_wait_for_comp(struct fid_cq *cq, uint64_t *cur,
			    uint64_t total, int timeout,
			    uint64_t tag)
{
	struct fi_cq_err_entry comp;
	int ret;

	while (total - *cur > 0) {
		ret = fi_cq_sread(cq, &comp, 1, NULL, timeout);
		if (ret > 0) {
			if (!ft_tag_is_valid(cq, &comp, tag ? tag : rx_cq_cntr))
				return -FI_EOTHER;
			(*cur)++;
		} else if (ret < 0 && ret != -FI_EAGAIN) {
			return ret;
		}
	}

	return 0;
}

/*
 * fi_cq_err_entry can be cast to any CQ entry format.
 */
static int ft_fdwait_for_comp(struct fid_cq *cq, uint64_t *cur,
			    uint64_t total, int timeout,
			    uint64_t tag)
{
	struct fi_cq_err_entry comp;
	struct fid *fids[1];
	int fd, ret;

	fd = cq == txcq ? tx_fd : rx_fd;
	fids[0] = &cq->fid;

	while (total - *cur > 0) {
		ret = fi_trywait(fabric, fids, 1);
		if (ret == FI_SUCCESS) {
			ret = ft_poll_fd(fd, timeout);
			if (ret && ret != -FI_EAGAIN)
				return ret;
		}

		ret = fi_cq_read(cq, &comp, 1);
		if (ret > 0) {
			if (!ft_tag_is_valid(cq, &comp, tag ? tag : rx_cq_cntr))
				return -FI_EOTHER;
			(*cur)++;
		} else if (ret < 0 && ret != -FI_EAGAIN) {
			return ret;
		}
	}

	return 0;
}

int ft_read_cq(struct fid_cq *cq, uint64_t *cur,
		uint64_t total, int timeout,
		uint64_t tag)
{
	int ret;

	switch (opts.comp_method) {
	case FT_COMP_SREAD:
	case FT_COMP_YIELD:
		ret = ft_wait_for_comp(cq, cur, total, timeout, tag);
		break;
	case FT_COMP_WAIT_FD:
		ret = ft_fdwait_for_comp(cq, cur, total, timeout, tag);
		break;
	default:
		ret = ft_spin_for_comp(cq, cur, total, timeout, tag);
		break;
	}
	return ret;
}

int ft_get_cq_comp(struct fid_cq *cq, uint64_t *cur,
		    uint64_t total, int timeout)
{
	int ret;

	ret = ft_read_cq(cq, cur, total, timeout, ft_tag);

	if (ret) {
		if (ret == -FI_EAVAIL) {
			ret = ft_cq_readerr(cq);
			(*cur)++;
		} else {
			FT_PRINTERR("ft_get_cq_comp", ret);
		}
	}
	return ret;
}

static int ft_spin_for_cntr(struct fid_cntr *cntr, uint64_t total, int timeout)
{
	struct timespec a, b;
	uint64_t cur;

	if (timeout >= 0)
		clock_gettime(CLOCK_MONOTONIC, &a);

	for (;;) {
		cur = fi_cntr_read(cntr);
		if (cur >= total)
			return 0;

		if (timeout >= 0) {
			clock_gettime(CLOCK_MONOTONIC, &b);
			if ((b.tv_sec - a.tv_sec) > timeout)
				break;
		}
	}

	fprintf(stderr, "%ds timeout expired\n", timeout);
	return -FI_ENODATA;
}

static int ft_wait_for_cntr(struct fid_cntr *cntr, uint64_t total, int timeout)
{
	int ret;

	while (fi_cntr_read(cntr) < total) {
		ret = fi_cntr_wait(cntr, total, timeout);
		if (ret)
			FT_PRINTERR("fi_cntr_wait", ret);
		else
			break;
	}
	return 0;
}

int ft_get_cntr_comp(struct fid_cntr *cntr, uint64_t total, int timeout)
{
	int ret = 0;

	switch (opts.comp_method) {
	case FT_COMP_SREAD:
	case FT_COMP_WAITSET:
	case FT_COMP_WAIT_FD:
	case FT_COMP_YIELD:
		ret = ft_wait_for_cntr(cntr, total, timeout);
		break;
	default:
		ret = ft_spin_for_cntr(cntr, total, timeout);
		break;
	}

	if (ret)
		FT_PRINTERR("fs_get_cntr_comp", ret);

	return ret;
}

int ft_get_rx_comp(uint64_t total)
{
	int ret = FI_SUCCESS;

	if (opts.options & FT_OPT_RX_CQ) {
		ret = ft_get_cq_comp(rxcq, &rx_cq_cntr, total, timeout);
	} else if (rxcntr) {
		ret = ft_get_cntr_comp(rxcntr, total, timeout);
	} else {
		FT_ERR("Trying to get a RX completion when no RX CQ or counter were opened");
		ret = -FI_EOTHER;
	}
	return ret;
}

int ft_get_tx_comp(uint64_t total)
{
	int ret;

	if (opts.options & FT_OPT_TX_CQ) {
		ret = ft_get_cq_comp(txcq, &tx_cq_cntr, total, -1);
	} else if (txcntr) {
		ret = ft_get_cntr_comp(txcntr, total, -1);
	} else {
		FT_ERR("Trying to get a TX completion when no TX CQ or counter were opened");
		ret = -FI_EOTHER;
	}
	return ret;
}

int ft_tx_msg(struct fid_ep *ep, fi_addr_t fi_addr, size_t size, void *ctx, uint64_t flags)
{
	int ret;

	if (ft_check_opts(FT_OPT_VERIFY_DATA | FT_OPT_ACTIVE)) {
		ret = ft_fill_buf((char *) tx_buf + ft_tx_prefix_size(), size);
		if (ret)
			return ret;
	}

	ret = ft_sendmsg(ep, fi_addr, size, ctx, flags);
	if (ret)
		return ret;

	ret = ft_get_tx_comp(tx_seq);
	return ret;
}

int ft_sendmsg(struct fid_ep *ep, fi_addr_t fi_addr,
		size_t size, void *ctx, int flags)
{
	struct fi_msg msg;
	struct fi_msg_tagged tagged_msg;
	struct iovec msg_iov;

	msg_iov.iov_base = tx_buf;
	msg_iov.iov_len = size + ft_tx_prefix_size();

	if (hints->caps & FI_TAGGED) {
		tagged_msg.msg_iov = &msg_iov;
		tagged_msg.desc = &mr_desc;
		tagged_msg.iov_count = 1;
		tagged_msg.addr = fi_addr;
		tagged_msg.data = NO_CQ_DATA;
		tagged_msg.context = ctx;
		tagged_msg.tag = ft_tag ? ft_tag : tx_seq;
		tagged_msg.ignore = 0;

		FT_POST(fi_tsendmsg, ft_progress, txcq, tx_seq,
			&tx_cq_cntr, "tsendmsg", ep, &tagged_msg,
			flags);
	} else {
		msg.msg_iov = &msg_iov;
		msg.desc = &mr_desc;
		msg.iov_count = 1;
		msg.addr = fi_addr;
		msg.data = NO_CQ_DATA;
		msg.context = ctx;

		FT_POST(fi_sendmsg, ft_progress, txcq, tx_seq,
			&tx_cq_cntr, "sendmsg", ep, &msg,
			flags);
	}

	return 0;
}

int ft_cq_readerr(struct fid_cq *cq)
{
	struct fi_cq_err_entry cq_err;
	int ret;

	memset(&cq_err, 0, sizeof(cq_err));
	ret = fi_cq_readerr(cq, &cq_err, 0);
	if (ret < 0) {
		FT_PRINTERR("fi_cq_readerr", ret);
	} else {
		FT_CQ_ERR(cq, cq_err, NULL, 0);
		ret = -cq_err.err;
	}
	return ret;
}

void eq_readerr(struct fid_eq *eq, const char *eq_str)
{
	struct fi_eq_err_entry eq_err;
	int rd;

	memset(&eq_err, 0, sizeof(eq_err));
	rd = fi_eq_readerr(eq, &eq_err, 0);
	if (rd != sizeof(eq_err)) {
		FT_PRINTERR("fi_eq_readerr", rd);
	} else {
		FT_EQ_ERR(eq, eq_err, NULL, 0);
	}
}

int ft_sync()
{
	char buf = 'a';
	int ret;

	if (opts.dst_addr) {
    ret = ft_tx_msg(ep, remote_fi_addr, 1, &tx_ctx, FI_DELIVERY_COMPLETE);
    if (ret)
      return ret;

    ret = ft_rx(ep, 1);
	} else {
    ret = ft_rx(ep, 1);
    if (ret)
      return ret;

    ret = ft_tx_msg(ep, remote_fi_addr, 1, &tx_ctx, FI_DELIVERY_COMPLETE);
    if (ret)
      return ret;
	}

	return ret;
}

int ft_finalize_ep(struct fid_ep *ep)
{
	int ret;
	struct fi_context ctx;

	ret = ft_sendmsg(ep, remote_fi_addr, 4, &ctx, FI_TRANSMIT_COMPLETE);
	if (ret)
		return ret;

	ret = ft_get_tx_comp(tx_seq);
	if (ret)
		return ret;

	ret = ft_get_rx_comp(rx_seq);
	if (ret)
		return ret;

	return 0;
}

int ft_finalize(void)
{
	int ret;

	if (fi->domain_attr->mr_mode & FI_MR_RAW) {
		ret = fi_mr_unmap_key(domain, remote.key);
		if (ret)
			return ret;
	}

	return ft_finalize_ep(ep);
}

int ft_fill_buf(void *buf, size_t size)
{
	char *msg_buf;
	int msg_index = 0;
	size_t i;
	int ret = 0;

	if (opts.iface != FI_HMEM_SYSTEM) {
		assert(dev_host_buf);
		msg_buf = dev_host_buf;
	} else {
		msg_buf = (char *) buf;
	}

	for (i = 0; i < size; i++) {
		msg_buf[i] = integ_alphabet[msg_index];
		if (++msg_index >= integ_alphabet_length)
			msg_index = 0;
	}

	if (opts.iface != FI_HMEM_SYSTEM) {
		ret = ft_hmem_copy_to(opts.iface, opts.device, buf, msg_buf, size);
		if (ret)
			goto out;
	}
out:
	return ret;
}

int ft_check_buf(void *buf, size_t size)
{
	char *recv_data;
	char c;
	int msg_index = 0;
	size_t i;
	int ret = 0;

	if (opts.iface != FI_HMEM_SYSTEM) {
		assert(dev_host_buf);
		ret = ft_hmem_copy_from(opts.iface, opts.device,
					dev_host_buf, buf, size);
		if (ret)
			return ret;
		recv_data = (char *)dev_host_buf;
	} else {
		recv_data = (char *)buf;
	}

	for (i = 0; i < size; i++) {
		c = integ_alphabet[msg_index];
		if (++msg_index >= integ_alphabet_length)
			msg_index = 0;
		if (c != recv_data[i])
			break;
	}
	if (i != size) {
		printf("Data check error (%c!=%c) at byte %zu for "
		       "buffer size %zu\n", c, recv_data[i], i, size);
		ret = -FI_EIO;
	}

	return ret;
}

int ft_rma_poll_buf(void *buf, int iter, size_t size)
{
	volatile char *recv_data;
	struct timespec a, b;

	if (opts.iface != FI_HMEM_SYSTEM) {
		FT_ERR("FI_HMEM not supported for write latency test");
		return EXIT_FAILURE;
	}

	recv_data = (char *)buf + size - 1;

	if (timeout >= 0)
		clock_gettime(CLOCK_MONOTONIC, &a);

	char expected_val = (char)iter;
	while (*recv_data != expected_val) {

		ft_force_progress();

		if (timeout >= 0) {
			clock_gettime(CLOCK_MONOTONIC, &b);
			if ((b.tv_sec - a.tv_sec) > timeout) {
				fprintf(stderr, "%ds timeout expired\n", timeout);
				return -FI_ENODATA;
			}
		}
	}

	return 0;
}

uint64_t ft_init_cq_data(struct fi_info *info)
{
	if (info->domain_attr->cq_data_size >= sizeof(uint64_t)) {
		return 0x0123456789abcdefULL;
	} else {
		return 0x0123456789abcdef &
			((0x1ULL << (info->domain_attr->cq_data_size * 8)) - 1);
	}
}

int ft_sock_send(int fd, void *msg, size_t len)
{
	size_t sent;
	ssize_t ret, err = 0;

	for (sent = 0; sent < len; ) {
		ret = ofi_send_socket(fd, ((char *) msg) + sent, len - sent, 0);
		if (ret > 0) {
			sent += ret;
		} else if (ofi_sockerr() == EAGAIN || ofi_sockerr() == EWOULDBLOCK) {
			ft_force_progress();
		} else {
			err = -ofi_sockerr();
			break;
		}
	}

	return err ? err: 0;
}

int ft_sock_recv(int fd, void *msg, size_t len)
{
	size_t rcvd;
	ssize_t ret, err = 0;

	for (rcvd = 0; rcvd < len; ) {
		ret = ofi_recv_socket(fd, ((char *) msg) + rcvd, len - rcvd, 0);
		if (ret > 0) {
			rcvd += ret;
		} else if (ret == 0) {
			err = -FI_ENOTCONN;
			break;
		} else if (ofi_sockerr() == EAGAIN || ofi_sockerr() == EWOULDBLOCK) {
			ft_force_progress();
		} else {
			err = -ofi_sockerr();
			break;
		}
	}

	return err ? err: 0;
}
