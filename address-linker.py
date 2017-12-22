import sqlite3
import csv
import time
from pprint import pprint
from blockcypher import get_address_details
from blockcypher import get_address_full

sqlfile = "address-linker.db"
csvfname = 'Addresses.csv'
conn = sqlite3.connect(sqlfile)

addresses = []


def db_connect():
    conn = sqlite3.connect(sqlfile)
    c = conn.cursor()
    return c

def db_init(conn):
    # Create table if not present
    conn.execute('''CREATE TABLE IF NOT EXISTS Addresses
          (Account text, Address text, Own INTEGER , TxCount INTEGER, Balance INTEGER)''')

    # Required for 'insert or replace' to work properly
    conn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS iaddress ON Addresses (Address)''')

def db_update_address(address_data):
    conn.execute("""INSERT OR REPLACE INTO Addresses (Account, Address, Own, TxCount, Balance)
          VALUES (?, ?, ?, ?, ?)""", address_data)
    conn.commit()

def scan_addresses(addresslist, maxrequests=10, delay=0):
    print('Sleep {} second(s) between requests'.format(delay))

    count = 0
    
    for row in addresslist:
        account = row[0]
        addr = row[1]
        
        # Prevent exceeding rate limit while testing
        if count >= maxrequests:
            return
        
        time.sleep(delay)
        
        addr_detail = get_address_full(addr)
        #pprint(addr_detail)

        txcount = addr_detail['final_n_tx']
        balance = addr_detail['balance']

        dbdata = [account, addr, '1', txcount, balance]
        db_update_address(dbdata)
        
        txs = addr_detail['txs']

        tx_addr = []
        for tx in txs:
            for a in tx['addresses']:
                tx_addr.append(a)
                if a not in addresses:
                    dbdata = [None, a, '0', None, None]
                    #print('Address not mine: {}'.format(a))
                    db_update_address(dbdata)

        print('Address: {}'.format(addr))
        print('Balance: {}'.format(balance))
        print('Associated Addresses: {}'.format(tx_addr))

        count = count + 1


dbc = db_connect()
db_init(dbc)

with open(csvfname, newline='') as csvfile:
    next(csvfile, None)  # skip the headers
    addresslist = csv.reader(csvfile, delimiter=',', quotechar='|')
    for row in addresslist:
        if row[1] not in addresses:
            addresses.append(row[1])
        #print(row[1])
        #print(', '.join(row))

    # Reset to beginning of file
    csvfile.seek(0)
    next(csvfile, None)  # skip the headers
    
    print('{} unique addresses found'.format(len(addresses)))
    
    scan_addresses(addresslist, 5, 0.75)
    print('Done')

dbc.close()



