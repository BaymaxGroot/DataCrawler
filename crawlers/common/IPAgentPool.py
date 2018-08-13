
from crawlers.common.db_operation import db_query

class IPAgentPool(object):

    def get_ip_agent():
        try:
            sql = """
                SELECT 
                    ip
                FROM
                    ip_agent
                WHERE 
                    enable = 1
            """
            return db_query(sql)
        except Exception as e:
            print("Failed to query the enabled ip agent list. Error - {}.".format(str(e)))
            return []