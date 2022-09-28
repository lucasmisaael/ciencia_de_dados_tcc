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
    
    @staticmethod
    def get_orders_data_linear_regression():
        query = """
                SELECT   
                    concat( o.order_created_year 
                                            ,'-'
                                            ,o.order_created_month
                                            ,'-'
                                            ,o.order_created_day
                                            , ' '
                                            ,order_created_hour
                                            , ':00')::timestamp  as order_created_at_hour,
                    order_created_day  as day_of_month,
                    o.channel_id,
                    case 
                        when extract(dow from concat(o.order_created_year, '-',o.order_created_month, '-', o.order_created_day, ' ',order_created_hour, ':00:00')::timestamp at time zone 'America/Sao_Paulo') in (5,6,0) then 1
                        else 0
                    end as is_weekend,
                    extract(dow from concat(o.order_created_year, '-',o.order_created_month, '-', o.order_created_day, ' ',order_created_hour, ':00:00')::timestamp at time zone 'America/Sao_Paulo') day_of_week,
                    order_created_hour  order_hour, 
                    order_created_month  order_month,
                    case 
                        when c.channel_type = 'MARKETPLACE' then 1 
                        else 2 
                    end as channel_type,
                    c.channel_type channel_type2,
                    case 
                        when hub_city  = 'CURITIBA' then 1 
                        else 0
                    end as hub_city_curitiba,
                    case 
                        when hub_city  = 'RIO DE JANEIRO' then 1
                        else 0
                    end hub_city_rj, 
                    case 
                        when hub_city  = 'PORTO ALEGRE' then 1
                        else 0
                    end hub_city_pa,
                    case 
                        when hub_state = 'RS' then 1
                        else 0
                    end as hub_state_rs,
                    case 
                        when hub_state = 'SP' then 1
                        else 0   
                    end as hub_state_sp,
                    case 
                        when hub_state = 'RJ' then 1
                        else 0
                    end as hub_state_rj,
                    count(order_id) as order_count
                FROM orders o 
                LEFT JOIN stores s
                    ON s.store_id = o.store_id 
                LEFT JOIN channels c
                    ON c.channel_id  = o.channel_id 
                LEFT JOIN hubs h 
                    ON h.hub_id  = s.hub_id 
                WHERE 
                    s.hub_id = 25
                AND  
                    date(to_char(concat(o.order_created_year, '-',o.order_created_month, '-', o.order_created_day, ' ',order_created_hour, ':00:00')::timestamp at time zone 'America/Sao_Paulo',  'yyyy-mm-dd hh24:00:00')) 
                    <= '2021-04-29'
                group by 
                    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
                order by 
                    day_of_month, 
                    order_hour
                """
        data = Connections.get_data(query)

        return data