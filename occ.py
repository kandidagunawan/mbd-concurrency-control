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
        self.timestamp = 0
        self.trans: Dict[int, Transaction] = {}
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
                    raise ValueError(f"INVALID SEQUENCE: {cmd}")

                parsed_cmd["tx"] = int(cmd[1:open_idx])                
                parsed_cmd["action"] = "read" if cmd[0].upper() == 'R' else "write"
                parsed_cmd["item"] = cmd[open_idx+1:close_idx]

            elif cmd[0].upper() == 'C':
                parsed_cmd["tx"] = int(cmd[1:])
                parsed_cmd["action"] = "commit"
            
            else:
                raise ValueError(f"INVALID COMMAND: {cmd}")

            parsed.append(parsed_cmd)
        return parsed
    

    def read(self, cmd):
        self.timestamp += 1

        tx = cmd["tx"]
        item = cmd["item"]

        if item not in self.trans[tx].reads:
            self.trans[tx].reads.append(item)
        
        reads = self.trans[tx].reads
        print(f"T={self.timestamp} | [READ]      T{tx} on {item} | READ_SET:{reads}")
    

    def temp_write(self, cmd):
        self.timestamp += 1

        tx = cmd["tx"]
        item = cmd["item"]

        if item not in self.trans[tx].writes:
            self.trans[tx].writes.append(item)

        writes = self.trans[tx].writes
        print(f"T={self.timestamp} | [TEMPWRITE] T{tx} on {item} | WRITE_SET:{writes}")
    

    def validate(self, cmd):
        self.timestamp += 1

        tx = cmd["tx"]

        self.trans[tx].timestamps["validation"] = self.timestamp
        is_valid = True

        for ti in self.trans.keys():
            if ti == tx: continue

            ti_validation = self.trans[ti].timestamps["validation"]
            ti_finish = self.trans[ti].timestamps["finish"]
            
            tx_start = self.trans[tx].timestamps["start"]
            tx_validation = self.trans[tx].timestamps["validation"]

            if ti_validation < tx_validation:
                if ti_finish < tx_start:
                    pass

                elif ti_finish != INF and tx_start < ti_finish and ti_finish < tx_validation:
                    ti_writes = self.trans[ti].writes
                    tx_reads = self.trans[tx].reads
                    
                    for var in ti_writes:
                        if var in tx_reads:
                            is_valid = False
                            break
                    
                    if not is_valid: break
                
                elif not set(self.trans[ti].writes).difference(self.trans[tx].reads):
                    print("meki")
                    is_valid = False
                    break
            
        if is_valid:
            print(f"T={self.timestamp} | [VALID]     T{tx} validation successful")
            self.write(tx)
        else:
            self.rolledback_nums.append(tx)
            print(f"T={self.timestamp} | [INVALID]   T{tx} validation failed")
            print(" " * len(str(self.timestamp)), end='')
            print(f"   | [ABORT]     T{tx} rolled back")
        

    def write(self, tx):
        t = self.trans[tx]
        for var in t.writes:
            print(" " * len(str(self.timestamp)), end='')
            print(f"   | [WRITE]     T{tx} on {var} to DB")
        self.commit(tx)

    
    def commit(self, tx):
        self.trans[tx].timestamps["finish"] = self.timestamp
        print(" " * len(str(self.timestamp)), end='')
        print(f"   | [COMMIT]    T{tx}")
    

    def rollback(self):
        while len(self.rolledback_nums) > 0:
            rolledback_tx = self.rolledback_nums.pop(0)
            rolledback_trans = self.trans[rolledback_tx]

            rolledback_trans.reset(self.timestamp)
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

            if tx not in self.trans.keys():
                self.trans[tx] = Transaction(tx)
                self.trans[tx].timestamps["start"] = self.timestamp

            self.action(cmd)
            
        self.rollback()
        print("Serial OCC Finished")



if __name__ == "__main__":
    seq = input("Enter input string (delimited by ;): ")

    try:
        occ = OCC(seq)
        occ.run()
    except ValueError as e:
        print(e)