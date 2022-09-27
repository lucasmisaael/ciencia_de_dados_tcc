import psycopg2 
import pandas as pd

class Connections: 

    @staticmethod
    def connect():
        conn = psycopg2.connect(
            host= "",
            database= "",
            user= "",
            password= "",
            port= "")
        conn.set_session( autocommit=True)
   
        return conn

    @staticmethod
    def get_data(query):

        # Abre Conexão com banco de dados 
        cursor = Connections.connect()
        data  = pd.read_sql(query,cursor)
        #Fecha conexão com banco de dados
        cursor.close()
        
        return data

    @staticmethod
    def get_orders_data():
        query = """
                SELECT  
                    concat( o.order_created_year 
                            ,'-'
                            ,o.order_created_month
                            ,'-'
                            ,o.order_created_day
                            , ' '
                            ,order_created_hour
                            , ':00')::timestamp  as order_created_at_hour
                    , count(order_id) as order_count
                FROM orders o 
                LEFT JOIN stores s
                    ON s.store_id = o.store_id 
                WHERE 1=1 
                AND
                    s.hub_id = 25
                group by 
                    o.order_created_year,
                    order_created_month ,
                    order_created_day,
                    order_created_hour 
                order by 
                    o.order_created_year,
                    order_created_month,
                    order_created_day, 
                    order_created_hour
                """
        data = Connections.get_data(query)

        return data