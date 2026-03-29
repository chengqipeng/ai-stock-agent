"""
调度状态持久化统一助手

各调度器调用 persist_status() 保存状态到数据库，
调用 restore_status() 在启动时恢复状态。
"""
import logging

logger = logging.getLogger(__name__)


def persist_status(job_id: str, main_fields: dict, dim_fields: dict = None):
    """
    保存调度状态到数据库。

    Args:
        job_id: 调度任务ID
        main_fields: 主记录字段 {'last_run_date', 'last_run_time', 'last_success', 'error'}
        dim_fields: 维度记录 {'kline': {'total':N, 'success':N, 'failed':N, 'extra_json':{}}, ...}
    """
    dims = {"_main": main_fields}
    if dim_fields:
        dims.update(dim_fields)

    try:
        from dao.scheduler_status_dao import save_job_status, create_table
        create_table()
        save_job_status(job_id, dims)
    except Exception as e:
        logger.warning("[状态持久化] %s 写入数据库失败: %s", job_id, e)


def restore_status(job_id: str) -> dict:
    """
    从数据库恢复调度状态。

    Returns:
        扁平化的状态字典，可直接 _job_status.update(result)
    """
    result = {}
    try:
        from dao.scheduler_status_dao import load_job_status, create_table
        create_table()
        db_data = load_job_status(job_id)
        if db_data:
            main = db_data.get("_main", {})
            result["last_run_date"] = main.get("last_run_date")
            result["last_run_time"] = main.get("last_run_time")
            result["last_success"] = main.get("last_success")
            if main.get("error"):
                result["error"] = main["error"]

            # 展开维度字段
            for dk, dv in db_data.items():
                if dk == "_main":
                    continue
                prefix = dk + "_"
                result[prefix + "total"] = dv.get("total", 0)
                result[prefix + "success"] = dv.get("success", 0)
                result[prefix + "failed"] = dv.get("failed", 0)
                if dv.get("skipped"):
                    result[prefix + "skipped"] = dv["skipped"]
                # extra_json 展开到顶层
                ej = dv.get("extra_json")
                if isinstance(ej, dict):
                    for ek, ev in ej.items():
                        result[ek] = ev

            if result.get("last_run_date"):
                logger.info("[状态恢复] %s 从数据库恢复: last_run_date=%s", job_id, result["last_run_date"])
    except Exception as e:
        logger.warning("[状态恢复] %s 数据库加载失败: %s", job_id, e)

    return result
