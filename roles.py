def validate_role(role:str,sql:str):

    sql=sql.strip().lower()
    role=role.upper()

    write_ops=("insert",
        "update",
        "delete",
        "create",
        "alter",
        "drop",
        "truncate")

    if role=="USER":
        if sql.startswith(write_ops):
            raise PermissionError("User role not allowed to modify data")
        
    if role=="ADMIN":
        return True
    
    raise PermissionError("Invalid role")
    



        
