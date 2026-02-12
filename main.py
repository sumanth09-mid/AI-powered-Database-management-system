from fastapi import FastAPI,HTTPException

from db import get_connection

from sql_engine import english_to_sql

from roles import validate_role

from kafka_producer import send_event


app=FastAPI()


@app.post("/query")
def run_query(payload:dict):

    try:
        role=payload["role"]

        query=payload["query"]

        sql=english_to_sql(query)

        validate_role(role,sql)


        conn=get_connection()

        cur=conn.cursor()
        cur.execute(sql)


        rows=cur.fetchall()

        cur.close()

        conn.close()

        send_event(role,query,sql,"Success")

        return {
            "sql_generate":sql,
            "result":rows
        }
    
    except PermissionError as e:
        send_event(role,query,sql,"Failed")

        raise HTTPException(status_code=403,detail=str(e))
    
    except Exception as e:
         send_event(role,query,sql,"Failed")
         raise HTTPException(status_code=400,detail=str(e))
    

