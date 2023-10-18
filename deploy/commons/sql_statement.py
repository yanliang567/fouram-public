from datetime import datetime

from deploy.commons.status_code import InstanceNodeStatus, OversoldStatus
from deploy.commons.common_func import gen_str

from db_client.client_mysql import ClientMySql


def sql_query_instance_class(mysql_client: ClientMySql, class_id, region_id, disk_type: int = None,
                             db="resource.instance_class"):
    sql = "SELECT * FROM {0} where class_id = '{1}' and region_id = '{2}'"
    sql += " and disk_type = {3};" if disk_type else ";"
    query_sql = sql.format(db, class_id, region_id, disk_type)
    return mysql_client.query(query_sql)


def sql_insert_instance_class(mysql_client: ClientMySql, class_id: str, cpu_cores: int, mem_size: int, region_id: str,
                              disk_type: int, db="resource.instance_class"):
    # class_id cpu_cores mem_size

    properties = "(class_id, class_code, region_id, cpu_cores, mem_size, disk_size, " + \
                 "is_display, status, create_time, last_update, ins_form, cu, disk_type)"
    values = "('{0}', '{1}', '{2}', {3}, {4}, 0, 0, 1, '{5}', '{5}', -1, -1, {6})"

    sql = "INSERT INTO {0} {1} VALUES{2};".format(db, properties, values)
    class_code = class_id.split('-')[1]
    insert_sql = sql.format(class_id, class_code, region_id, cpu_cores, mem_size,
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), disk_type)

    if len(sql_query_instance_class(mysql_client, class_id, region_id, disk_type, db)) == 0:
        return mysql_client.insert(insert_sql)


def sql_query_cust_instance_node(mysql_client: ClientMySql, instance_id: str, db="resource.cust_instance_node"):
    sql = "SELECT * FROM {0} where instance_id = '{1}';"
    query_sql = sql.format(db, instance_id)
    return mysql_client.query(query_sql)


def sql_delete_cust_instance_node(mysql_client: ClientMySql, instance_id: str, db="resource.cust_instance_node"):
    sql = "DELETE FROM {0} where instance_id = '{1}';"
    delete_sql = sql.format(db, instance_id)
    return mysql_client.delete(delete_sql)


def sql_insert_cust_instance_node(mysql_client: ClientMySql, instance_id: str, cpu_cores: int, mem_size: int,
                                  class_id: str, category: int, region_id: str, create_time: str,
                                  status=InstanceNodeStatus.RUNNING,
                                  db="resource.cust_instance_node"):
    # node_id node_name cpu_cores mem_size class_id status =1

    properties = "(node_id, node_name, instance_id, cpu_cores, mem_size, disk_size, " + \
                 "class_id, category, region_id, status, create_time, last_update)"
    values = "('{0}', '{1}', '{2}', {3}, {4}, 0, '{5}', {6}, '{7}', {8}, '{9}', '{10}')"

    sql = "INSERT INTO {0} {1} VALUES{2};".format(db, properties, values)

    node_id = "no{0}-{1}".format(str(category).zfill(2), gen_str(15)).lower()
    # node_name = str(uuid.uuid1())[:32]
    node_name = gen_str(32)
    last_update = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    insert_sql = sql.format(node_id, node_name, instance_id, cpu_cores, mem_size, class_id,
                            category, region_id, status, create_time, last_update)

    return mysql_client.insert(insert_sql)


def sql_query_instance_class_oversold_class_id(
        mysql_client: ClientMySql, class_id: str, db="resource.instance_class_oversold",
        other_params: str = " and node_category is null and user_id is null and instance_id is null"):
    sql = "SELECT * FROM {0} where class_id = '{1}' " + other_params + ";"
    query_sql = sql.format(db, class_id)
    return mysql_client.query(query_sql)


def sql_query_instance_class_oversold(mysql_client: ClientMySql, instance_id: str,
                                      db="resource.instance_class_oversold"):
    sql = "SELECT * FROM {0} where instance_id = '{1}';"
    query_sql = sql.format(db, instance_id)
    return mysql_client.query(query_sql)


def sql_delete_instance_class_oversold(mysql_client: ClientMySql, instance_id: str,
                                       db="resource.instance_class_oversold"):
    sql = "DELETE FROM {0} where instance_id = '{1}';"
    delete_sql = sql.format(db, instance_id)
    return mysql_client.delete(delete_sql)


def sql_insert_instance_class_oversold(mysql_client: ClientMySql, instance_id: str, user_id: str, class_id: str,
                                       node_category: int, extend_fields: str, status=OversoldStatus.READY,
                                       db="resource.instance_class_oversold"):
    properties = "(instance_id, user_id, class_id, node_category, extend_fields, status, create_time, last_update)"
    values = "('{0}', '{1}', '{2}', {3}, \"{4}\", {5}, '{6}', '{6}')"
    sql = "INSERT INTO {0} {1} VALUES{2}".format(db, properties, values)

    _time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    insert_sql = sql.format(instance_id, user_id, class_id, node_category, extend_fields, status, _time)
    return mysql_client.insert(insert_sql)


def sql_query_child_class_template(mysql_client: ClientMySql, child_class_id: str, node_category: int,
                                   db="resource.child_class_template"):
    sql = "SELECT * FROM {0} where child_class_id = '{1}' and node_category = {2};"
    query_sql = sql.format(db, child_class_id, node_category)
    return mysql_client.query(query_sql)


def sql_delete_child_class_template(mysql_client: ClientMySql, child_class_id: str, node_category: int,
                                    db="resource.child_class_template"):
    sql = "DELETE FROM {0} where child_class_id = '{1}' and node_category = {2};"
    delete_sql = sql.format(db, child_class_id, node_category)
    return mysql_client.delete(delete_sql)


def sql_insert_child_class_template(mysql_client: ClientMySql, child_class_id: str, node_category: int,
                                    spec_content: str, db="resource.child_class_template"):
    properties = "(child_class_id, node_category, spec_content, create_time, last_update)"
    values = "('{0}', {1}, '{2}', '{3}', '{3}')"
    insert_sql = "INSERT INTO {0} {1} VALUES{2}".format(db, properties, values)

    # delete msg before insert
    if len(sql_query_child_class_template(mysql_client, child_class_id, node_category)) != 0:
        sql_delete_child_class_template(mysql_client, child_class_id, node_category)

    _time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    insert_sql = insert_sql.format(child_class_id, node_category, spec_content, _time)
    return mysql_client.insert(insert_sql)


""" check components' pod resources """


def sql_query_child_instance_class(mysql_client: ClientMySql, parent_class_id: str, region_id: int,
                                   db="resource.child_instance_class"):
    sql = "SELECT * FROM {0} where parent_class_id = '{1}' and region_id = '{2}';"
    query_sql = sql.format(db, parent_class_id, region_id)
    return mysql_client.query(query_sql)


def sql_query_child_instance_class_with_global(mysql_client: ClientMySql, parent_class_id: str, region_id: int,
                                               db="resource.child_instance_class_with_global"):
    sql = "SELECT * FROM {0} where parent_class_id = '{1}' and region_id = '{2}';"
    query_sql = sql.format(db, parent_class_id, region_id)
    return mysql_client.query(query_sql)
