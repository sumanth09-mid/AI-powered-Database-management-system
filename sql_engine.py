def english_to_sql(query:str):

    q=query.lower()

    if "all user" in q:

        return "Select * from employees"
    
    if "employees in nellore":

        return "select * from employees where city='nellore' "
    
    if "high salary":
        return "select * from employees where salary>60000"
    
    raise ValueError("Query not understood")