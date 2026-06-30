#! /usr/bin/env sh

echo "Setting up Oracle sample datadabase. https://github.com/oracle-samples/db-sample-schemas"

wget -O /tmp/sample-oracle-db.zip https://github.com/oracle-samples/db-sample-schemas/archive/refs/tags/v23.3.zip
unzip /tmp/sample-oracle-db.zip -d /tmp/
cd /tmp/db-sample-schemas-23.3

until echo "exit" | sqlplus -s sys/"${ORACLE_PASSWORD}"@${ORACLE_PDB} as sysdba; do
  sleep 10
  echo "Waiting for the Oracle database to start..."
done
 

cd /tmp/db-sample-schemas-23.3/human_resources
export ORACLE_PDB=$ORACLE_PDB
export ORACLE_PASSWORD=$ORACLE_PASSWORD

/usr/bin/expect << EOF1
    spawn sqlplus sys/${ORACLE_PASSWORD}@${ORACLE_PDB} as sysdba @hr_install.sql
    set timeout -1
    expect {
        -exact "Enter a password for the user HR: " {
            send "hr\r"   
            exp_continue  
        }

        -exact "Enter a tablespace for HR \\[USERS\\]: " {
            send "\r"     
            exp_continue
        }

        -exact "Do you want to overwrite the schema, if it already exists\\? \\[YES|no\\]: " {
            send "YES\r"  
            exp_continue
        }
        eof 
    }
EOF1



cd /tmp/db-sample-schemas-23.3/customer_orders
/usr/bin/expect << EOF2
    sqlplus sys/"${ORACLE_PASSWORD}"@${ORACLE_PDB} as sysdba @co_install.sql
    set timeout -1
    expect {
        -exact "Enter a password for the user CO: " {
            send "co\r"   
            exp_continue  
        }

        -exact "Enter a tablespace for HR \\[USERS\\]: " {
            send "\r"     
            exp_continue
        }

        -exact "Do you want to overwrite the schema, if it already exists\\? \\[YES|no\\]: " {
            send "YES\r"  
            exp_continue
        }
        eof 
    }
EOF2
 

cd /tmp/db-sample-schemas-23.3/sales_history
/usr/bin/expect << EOF3
    sqlplus sys/"${ORACLE_PASSWORD}"@${ORACLE_PDB} as sysdba @sh_install.sql
    set timeout -1
    expect {
        -exact "Enter a password for the user SH: " {
            send "sh\r"   
            exp_continue  
        }

        -exact "Enter a tablespace for HR \\[USERS\\]: " {
            send "\r"     
            exp_continue
        }

        -exact "Do you want to overwrite the schema, if it already exists\\? \\[YES|no\\]: " {
            send "YES\r"  
            exp_continue
        }
        eof 
    }
EOF3



echo "Sample data imported."