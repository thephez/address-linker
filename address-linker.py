import sqlite3
import csv
import time
from pprint import pprint
from blockcypher import get_address_details
from blockcypher import get_address_full

sqlfile = "address-linker.db"
csvfname = 'Addresses.csv'

addresses = []


def db_connect():
    conn = sqlite3.connect(sqlfile)
    return conn

def db_init(conn):
    # Create table if not present
    conn.execute('''CREATE TABLE IF NOT EXISTS Addresses
          (Account text, Address text, Own INTEGER , TxCount INTEGER, Balance INTEGER)''')

    # Required for 'insert or replace' to work properly
    conn.execute('''CREATE UNIQUE INDEX IF NOT EXISTS iaddress ON Addresses (Address)''')

def db_update_address(conn, address_data):
    conn.execute("""INSERT OR REPLACE INTO Addresses (Account, Address, Own, TxCount, Balance)
          VALUES (?, ?, ?, ?, ?)""", address_data)
    conn.commit()

def db_check_address(conn, address):
    # Returns 1 if address found, 0 otherwise
    cur = conn.cursor()
    cur.execute("SELECT EXISTS(SELECT 1 FROM Addresses WHERE Address=? AND Account IS NOT null LIMIT 1)", (address,))

    found = cur.fetchone()[0]
    return found

def db_get_account_balances(conn):
    cur = conn.cursor()
    cur.execute("SELECT Account, SUM(Balance), (SUM(Balance)*1.0/100000000) FROM Addresses WHERE balance > 0 GROUP BY Account")

    balances = cur.fetchall()
    print('Balance summary')
    for row in balances:
        print(row)

    return

def scan_addresses(addresslist, conn, maxrequests=10, delay=0):
    print('Sleep {} second(s) between requests'.format(delay))

    count = 0
    skip_count = 0

    for row in addresslist:
        account = row[0]
        addr = row[1]

        if db_check_address(conn, addr) == 1:
            skip_count = skip_count + 1
            print('{0}\tAddress already in database, skipping request ({1}) ...'.format(skip_count, addr))
            continue

        # Prevent exceeding rate limit while testing
        if count >= maxrequests:
            print('Reached max request limit ({}). Stopping requests ...'.format(maxrequests))
            print('{} addresses skipped'.format(skip_count))
            return

        time.sleep(delay)

        addr_detail = get_address_full(addr)

        txcount = addr_detail['final_n_tx']
        balance = addr_detail['balance']

        dbdata = [account, addr, '1', txcount, balance]
        db_update_address(conn, dbdata)

        txs = addr_detail['txs']

        tx_addr = []
        for tx in txs:
            for a in tx['addresses']:
                tx_addr.append(a)
                if a not in addresses:
                    dbdata = [None, a, '0', None, None]
                    #print('Address not mine: {}'.format(a))
                    db_update_address(conn, dbdata)

        print('Address: {0}\tAccount: {1}'.format(addr, account))
        print('Balance: {}'.format(balance))
        pprint('Associated Addresses: {}'.format(tx_addr))

        count = count + 1

def main():
    conn = sqlite3.connect(sqlfile)
    conn = db_connect()
    db_init(conn)

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

        try:
            scan_addresses(addresslist, conn, 250, 1)

        except Exception as e:
            print('\nException: {}\n'.format(e))

        db_get_account_balances(conn)

        print('Done')

    conn.close()


if __name__ == '__main__':
    main()
