from typing import Dict
import math

INF = math.inf

class Transaction:
    def __init__(self, tx_num: int) -> None:
        self.tx_num = tx_num
        self.reads = []
        self.writes = []
        self.timestamps = {
            "start": INF,
            "validation": INF,
            "finish": INF
        }

    def reset(self, time: int) -> None:
        self.reads = []
        self.writes = []
        self.timestamps = {
            "start": time,
            "validation": INF,
            "finish": INF
        }


class OCC:
    def __init__(self, input_sequence: str) -> None:
        self.sequence = self.parse_input(input_sequence)
        self.variables = {}
        self.curr_timestamp = 0
        self.transactions: Dict[int, Transaction] = {}
        self.rolledback_nums = []

    def parse_input(self, input_sequence: str):
        parsed = []
        seq = input_sequence.replace(' ', '').split(';')

        for cmd in seq:
            parsed_cmd = {}
            if cmd[0].upper() in ['R', 'W'] and '(' in cmd and ')' in cmd:
                open_idx = cmd.index('(')
                close_idx = cmd.index(')')

                if open_idx > close_idx:
                    print(f"INVALID SEQUENCE: {cmd}")
                    break

                parsed_cmd["tx"] = int(cmd[1:open_idx])                
                parsed_cmd["action"] = "read" if cmd[0].upper() == 'R' else "write"
                parsed_cmd["item"] = cmd[open_idx+1:close_idx]

            elif cmd[0].upper() == 'C':
                parsed_cmd["tx"] = int(cmd[1:])
                parsed_cmd["action"] = "commit"
            
            else:
                print(f"INVALID COMMAND: {cmd}")
                break

            parsed.append(parsed_cmd)
        return parsed
    
    def read(self, cmd):
        self.curr_timestamp += 1

        tx = cmd["tx"]
        item = cmd["item"]

        if item not in self.transactions[tx].reads:
            self.transactions[tx].reads.append(item)
        
        reads = self.transactions[tx].reads
        print(f"T={self.curr_timestamp} | [READ]      T{tx} on {item} | READ_SET:{reads}")
    
    def temp_write(self, cmd):
        self.curr_timestamp += 1

        tx = cmd["tx"]
        item = cmd["item"]

        if item not in self.transactions[tx].writes:
            self.transactions[tx].writes.append(item)

        writes = self.transactions[tx].writes
        print(f"T={self.curr_timestamp} | [TEMPWRITE] T{tx} on {item} | WRITE_SET:{writes}")
    

    def validate(self, cmd):
        self.curr_timestamp += 1

        tx = cmd["tx"]

        self.transactions[tx].timestamps["validation"] = self.curr_timestamp
        is_valid = True

        for ti in self.transactions.keys():
            if ti == tx: continue

            ti_validation = self.transactions[ti].timestamps["validation"]
            ti_finish = self.transactions[ti].timestamps["finish"]
            
            tx_start = self.transactions[tx].timestamps["start"]
            tx_validation = self.transactions[tx].timestamps["validation"]

            if ti_validation < tx_validation:
                if ti_finish < tx_start:
                    pass

                elif ti_finish != INF and tx_start < ti_finish and ti_finish < tx_validation:
                    ti_writes = self.transactions[ti].writes
                    tx_reads = self.transactions[tx].reads
                    
                    for var in ti_writes:
                        if var in tx_reads:
                            is_valid = False
                            break
                    
                    if not is_valid: break
                
                elif not set(self.transactions[ti].writes).difference(self.transactions[tx].reads):
                    print("meki")
                    is_valid = False
                    break
            
        if is_valid:
            print(f"T={self.curr_timestamp} | [VALID]     T{tx} validation successful")
            self.write(tx)
        else:
            self.rolledback_nums.append(tx)
            print(f"T={self.curr_timestamp} | [INVALID]   T{tx} validation failed")
            print(" " * len(str(self.curr_timestamp)), end='')
            print(f"   | [ABORT]     T{tx} rolled back")
        

    def write(self, tx):
        t = self.transactions[tx]
        for var in t.writes:
            print(" " * len(str(self.curr_timestamp)), end='')
            print(f"   | [WRITE]     T{tx} on {var} to DB")
        self.commit(tx)

    
    def commit(self, tx):
        self.transactions[tx].timestamps["finish"] = self.curr_timestamp
        print(" " * len(str(self.curr_timestamp)), end='')
        print(f"   | [COMMIT]    T{tx}")
    

    def rollback(self):
        while len(self.rolledback_nums) > 0:
            rolledback_tx = self.rolledback_nums.pop(0)
            rolledback_t = self.transactions[rolledback_tx]

            rolledback_t.reset(self.curr_timestamp)
            rolledback_cmd = []

            for cmd in self.sequence:
                if cmd["tx"] == rolledback_tx:
                    rolledback_cmd.append(cmd)

            for cmd in rolledback_cmd:
                self.action(cmd)
    

    def action(self, cmd):
        if cmd["action"] == "read":
            self.read(cmd)
        elif cmd["action"] == "write":
            self.temp_write(cmd)
        elif cmd["action"] == "commit":
            self.validate(cmd)

    def run(self):
        print("\nRunning Serial OCC Algorithm")
        for cmd in self.sequence:
            tx = cmd["tx"]

            if tx not in self.transactions.keys():
                self.transactions[tx] = Transaction(tx)
                self.transactions[tx].timestamps["start"] = self.curr_timestamp

            self.action(cmd)
            
        self.rollback()
        print("Serial OCC Finished")


if __name__ == "__main__":
    seq = input("Enter input string (delimited by ;): ")
    occ = OCC(seq)
    occ.run()