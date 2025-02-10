from typing import List, Tuple
from locust.stats import RequestStats, StatsEntry, STATS_TYPE_WIDTH, get_readable_percentiles, PERCENTILES_TO_REPORT

from client.common.common_func import check_max_value
from utils.util_log import log

NAME_MAX_LEN, NAME_LEN_BUFFER = 15, 40
NUM_MAX_LEN, NUM_LEN_BUFFER = 7, 3


def print_stats(stats: RequestStats, current=True) -> None:
    for line in get_stats_summary(stats, current):
        log.info(line)
    log.info("")


def to_string(stats: StatsEntry, current=True) -> (Tuple[str], int, int):
    """
    Override the to_string method of StatsEntry
    """
    if current:
        rps = stats.current_rps
        fail_per_sec = stats.current_fail_per_sec
    else:
        rps = stats.total_rps
        fail_per_sec = stats.total_fail_per_sec

    num = (
        f"{stats.num_requests}",
        "%d(%.2f%%)" % (stats.num_failures, stats.fail_ratio * 100),
        "%.0f" % stats.avg_response_time,
        "%.0f" % (stats.min_response_time or 0),
        "%.0f" % stats.max_response_time,
        "%.0f" % (stats.median_response_time or 0),
        "%.2f" % (rps or 0),
        "%.2f" % (fail_per_sec or 0),
    )
    return (
               (stats.method and stats.method + " " or ""),
               stats.name,
               *num
           ), len(stats.name), max([len(n) for n in num])


def get_stats_summary(stats: RequestStats, current=True) -> List[str]:
    """
    stats summary will be returned as list of string
    """
    # get all data
    all_data, name_max_len, num_max_len = [], NAME_MAX_LEN, NUM_MAX_LEN
    for key in sorted(stats.entries.keys()):
        _str, name_len, num_len = to_string(stats.entries[key], current=current)
        all_data.append(_str)
        num_max_len, name_max_len = check_max_value(num_max_len, num_len), check_max_value(name_max_len, name_len)
    # get aggregated data
    agg_data, agg_name_len, agg_num_len = to_string(stats.total, current=current)

    # set string length
    num_max_len = check_max_value(num_max_len, agg_num_len) + NUM_LEN_BUFFER
    name_max_len = check_max_value(name_max_len, agg_name_len) + NAME_LEN_BUFFER

    # set string format
    content_str_format = "%-{0}s %-{1}s %{2}s %{2}s |%{2}s %{2}s %{2}s %{2}s | %{2}s %{2}s".format(
        STATS_TYPE_WIDTH * 2, name_max_len, num_max_len)
    separator_str_format = "{0}|{1}|{2}|-{2}|{2}|{2}|{2}|{2}-|-{2}|{2}".format(
        "-" * STATS_TYPE_WIDTH * 2, "-" * name_max_len, "-" * num_max_len)

    return [
        content_str_format % ("Type", "Name", "# reqs", "# fails", "Avg", "Min", "Max", "Med", "req/s", "failures/s"),
        separator_str_format,
        *[content_str_format % i for i in all_data],
        separator_str_format,
        content_str_format % agg_data,
    ]


def print_percentile_stats(stats: RequestStats) -> None:
    for line in get_percentile_stats_summary(stats):
        log.info(line)
    log.info("")


def percentile(stats: StatsEntry) -> (Tuple[str], int, int):
    """
    Override the percentile method of StatsEntry
    """
    if not stats.num_requests:
        raise ValueError("Can't calculate percentile on url with no successful requests")

    res = tuple(f"%d" % stats.get_response_time_percentile(p) for p in PERCENTILES_TO_REPORT)
    return (
               stats.method or "",
               stats.name,
               *res,
               stats.num_requests,
           ), len(stats.name), max([len(r) for r in res])


def get_percentile_stats_summary(stats: RequestStats) -> List[str]:
    """
    Percentile stats summary will be returned as list of string
    """
    # get all data
    all_data, name_max_len, num_max_len = [], NAME_MAX_LEN, NUM_MAX_LEN
    for key in sorted(stats.entries.keys()):
        r = stats.entries[key]
        if r.response_times:
            _str, name_len, num_len = percentile(r)
            all_data.append(_str)
            num_max_len, name_max_len = check_max_value(num_max_len, num_len), check_max_value(name_max_len, name_len)
    # get aggregated data
    agg_data = ()
    if stats.total.response_times:
        agg_data, agg_name_len, agg_num_len = percentile(stats.total)
        # check len
        num_max_len = check_max_value(num_max_len, agg_num_len)
        name_max_len = check_max_value(name_max_len, agg_name_len)

    # set string length
    num_max_len += NUM_LEN_BUFFER
    name_max_len += NAME_LEN_BUFFER

    # set string format
    content_str_format = "%-{0}s %-{1}s{2}".format(
        STATS_TYPE_WIDTH * 2, name_max_len, f" %{num_max_len}s" * (len(PERCENTILES_TO_REPORT) + 1))
    separator_str_format = "{0}|{1}|{2}|{2}|{2}|{2}|{2}|{2}|{2}|{2}|{2}|{2}|{2}|{2}".format(
        "-" * STATS_TYPE_WIDTH * 2, "-" * name_max_len, "-" * num_max_len)

    summary = [
        "Response time percentiles (approximated)",
        content_str_format % (("Type", "Name") + tuple(get_readable_percentiles(PERCENTILES_TO_REPORT)) + ("# reqs",)),
        separator_str_format,
        *[content_str_format % i for i in all_data],
        separator_str_format,
    ]
    if agg_data:
        summary.append(content_str_format % agg_data)
    return summary
