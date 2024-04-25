import logging
import pymysql
from sshtunnel import SSHTunnelForwarder
import paramiko
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# SSH Connection details
ssh_host = os.getenv('ssh_host')
ssh_port = 22
ssh_user = os.getenv('ssh_user')
ssh_key_file = os.getenv('ssh_key_file')
ssh_passphrase = os.getenv('ssh_passphrase')

# MySQL Connection details
mysql_host =  os.getenv('mysql_host') # Localhost because SSH tunnel is forwarded to local port
mysql_port = 3306  # Local port forwarded through SSH tunnel
mysql_user = os.getenv('mysql_user')
mysql_password = os.getenv('mysql_password')
mysql_database = 'staging'

# Create SSH tunnel
with SSHTunnelForwarder(
    (ssh_host, ssh_port),
    ssh_username=ssh_user,
    ssh_pkey=paramiko.RSAKey.from_private_key_file(ssh_key_file, password=ssh_passphrase),
    remote_bind_address=(mysql_host, mysql_port)
) as tunnel:
    # Connect to MySQL through the SSH tunnel
    try:
        conn = pymysql.connect(
            host=mysql_host,
            port=tunnel.local_bind_port,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database,
        )
        logger.debug("Connected to MySQL through SSH tunnel")

        # Example query
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM icd_diagnosis")
            result = cursor.fetchall()
            logger.debug("Query executed successfully")
            logger.debug(result)

    except Exception as e:
        logger.error(f"Error connecting to MySQL: {e}")

    finally:
        # Close the MySQL connection
        conn.close()
        logger.debug("MySQL connection closed")
