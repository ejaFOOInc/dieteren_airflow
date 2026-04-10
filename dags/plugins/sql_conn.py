
def get_connection_string(
    server :str,
    database :str,
    isEncryptedConnection :bool,
    isTrustedServerCertificate :bool,
    connectionTimeout :int
):
    
    """
    
    """
    return (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={server};"
        f"Database={database};"
        f"Encrypt={isEncryptedConnection};"
        f"TrustServerCertificate={isTrustedServerCertificate};"
        f"Connection Timeout={connectionTimeout};"
    )