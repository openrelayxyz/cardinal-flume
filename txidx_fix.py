import sys

def main(file):

    with open(file, 'r') as f:
        lines = [line.strip().split(':') for line in f]

    output = open('txidx-statements.txt', 'w')

    for line in lines:
        s = f"UPDATE event_logs SET transactionIndex = {line[1]} WHERE transactionIndex = X'{line[0]}';"
        output.write(s + '\n')
    
    output.close()

if __name__ == "__main__":
    main(sys.argv[1])

# I am saving the below for posterity so that we can look back on how the data was captured if we need to

# q = 'SELECT DISTINCT(HEX(transactionIndex)) FROM event_logs where block <= 554114;'
# initial = conn.fetchall(q)
# statements = [item[0] for item in initial_results]

# output = open(f'fix.txt', "w")

# for s in statements:
# output.write(s)
# output.write(':')
# output.write(str(int(s[:8], 16)))
# output.write('\n')

# output.close()