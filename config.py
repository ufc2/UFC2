# server config
port = '6060'
one_day_in_seconds = 24 * 60 * 60
path_join = '/'

# s3 config
s3_config = { 
    'access_key': '', 
    'secret_key': '', 
    'region': '', 
    'bucket': '' 
    }
s3_max_threads = 100

# efs config
efs_root = '/home/ec2-user/efs/getting-started'
efs_file_root = efs_root + path_join + 'namespaces'

# sqlite3 config
sqlite3_db_path = efs_root + path_join + 'ufc2.db'

server_user = { 
    'user_id': -1, 
    'email': 'server@server', 
    'firstname': 'server', 
    'lastname': 'server' 
    }
