from queue import SimpleQueue
from queue import Queue

class Transaction:
    def __init__(self, transaction_number, timestamp):
        self.transaction_number= transaction_number
        self.time_stamp = timestamp
        self.operation_queue = []
        self.exclusive_lock = []
        self.shared_lock = []
    def rollback_transaction(self):
        self.operation_queue = []
        self.exclusive_lock = []
        self.shared_lock = []
    def add_operation_queue(self, operation):
        self.operation_queue.append(operation)
        print(f"Transaction {self.transaction_number} queue: {self.operation_queue}")
    def add_exclusive_lock(self, resource):
        self.exclusive_lock.append(resource)
        print(f"Transaction {self.transaction_number} acquired exclusive lock on {resource}")
    def add_shared_lock(self, resource):
        self.shared_lock.append(resource)
        print(f"Transaction {self.transaction_number} acquired shared lock on {resource}")
    def unlock_all_resources(self):
        print(f"Transaction {self.transaction_number} released exclusive lock on {self.exclusive_lock}")
        print(f"Transaction {self.transaction_number} released shared lock on {self.shared_lock}")
        unlock_string = ""
        all_unlock = []
        for i in self.exclusive_lock:
            unlock_string = "Unlock-X(" + i + ")"
            all_unlock.append(unlock_string)
        for j in self.shared_lock:
            unlock_string = "Unlock-S(" + j + ")"
            all_unlock.append(unlock_string)
        self.exclusive_lock = []
        self.shared_lock = []
        return all_unlock

        
    def is_resource_shared_locked(self, resource):
        for r in self.shared_lock:
            if r == resource:
                return True
        return False
    def is_resource_exclusive_locked(self, resource):
        for r in self.exclusive_lock:
            if r == resource:
                return True
        return False

class Schedule:
    def __init__(self, schedule_list):
        self.schedule_list = schedule_list
        self.transaction_set = set()
        self.transaction_number_set = set()
        self.pending_transaction = set()
        self.operation_queue = []
        self.previous_operation_queue = []
        self.final_schedule = []
        for s in schedule_list:
            if(s[0] == 'R' or s[0] == 'W'):
                if(s.split("(")[0][1:] not in self.transaction_number_set):
                    self.transaction_number_set.add(s.split("(")[0][1:])
                    new_transaction = Transaction(transaction_number= s.split("(")[0][1:], timestamp = s.split("(")[0][1:])
                    self.transaction_set.add(new_transaction)
    def abort_transaction(self, transaction_number):
        t_list = []
        for operation_transaction in list(self.final_schedule):
            temp_X = "Lock-X" + str(transaction_number)  + "(" + self.get_resource(operation_transaction) + ")"
            temp_S = "Lock-S" + str(transaction_number)  + "(" + self.get_resource(operation_transaction) + ")"
            self.pending_transaction = set()
            if(operation_transaction[0] != 'C' and operation_transaction[0] != "L" and self.get_transaction_number(operation_transaction) == transaction_number):
                print("masuk1")
                t_list.append(operation_transaction)
                self.final_schedule.remove(operation_transaction)
            elif(self.get_transaction_number(operation_transaction) == transaction_number):
                print("masuk2")
                self.final_schedule.remove(operation_transaction)
            # elif(operation_transaction[0] == "L" and (operation_transaction.split("(")):
            #     self.final_schedule.remove(operation_transaction)
            elif(operation_transaction== temp_X or operation_transaction == temp_S):
                print("masuk3")
                self.final_schedule.remove(operation_transaction)
            
            
        self.get_transaction(transaction_number=transaction_number).rollback_transaction()
        self.schedule_list = self.operation_queue
        self.operation_queue = []
        for t in range(len(t_list)-1, -1, -1):
            self.get_transaction(transaction_number=transaction_number).operation_queue.insert(0, t_list[t])
            self.operation_queue.insert(0, t_list[t])
        self.previous_operation_queue = self.operation_queue

        temp = self.operation_queue
        # self.operation_queue.insert(0, t_list)
       
            
    def get_transaction(self, transaction_number):
        for transaction in self.transaction_set:
            if transaction.transaction_number == transaction_number:
                return transaction
    def is_resource_free(self, resource, transaction_number):
        avail = True
        for transaction in self.transaction_set:
            if(transaction.is_resource_shared_locked(resource = resource) or transaction.is_resource_exclusive_locked(resource = resource) and transaction.transaction_number != transaction_number):
                avail = False
                break
        return avail
    def is_resource_only_shared_locked(self, resource):
        avail = True
        for transaction in self.transaction_set:
            if(transaction.is_resource_exclusive_locked(resource = resource)):
                avail = False
                break
        return avail

    def is_acquired_by_transaction_number(self, resource, access, transaction_number):
        transaction = self.get_transaction(transaction_number)
        if(access == 'shared'):
            return transaction.is_resource_shared_locked(resource = resource)
        else:
            return transaction.is_resource_exclusive_locked(resource = resource)
    def get_access_type(self, resource, transaction_number):
        for s in self.schedule_list:
            _transaction_number = self.get_transaction_number(s)
            _resource = self.get_resource(s)
            _operation = self.get_operation(s)
            if _transaction_number == transaction_number and _resource == resource:
                if(_operation == 'W'):
                    return 'exclusive'
        return 'shared'
            
    def get_transaction_number(self, operation_transaction):
        if(operation_transaction[0] == 'R' or operation_transaction[0] == 'W'):
            return operation_transaction[1:].split("(")[0]
        else:
            return operation_transaction[1:]
    def get_operation(self, operation_transaction):
        return operation_transaction[0]
    def get_resource(self, operation_transaction):
        if(operation_transaction[0] == 'R' or operation_transaction[0] == 'W' or operation_transaction[0] == "L" or operation_transaction[0] == "U"):
            return operation_transaction.split("(")[1].split(")")[0]
        else:
            return ""
    def add_pending_transaction(self, operation_transaction):
        print(f"Transanction {operation_transaction} is added to pending transaction queue")
        self.pending_transaction.add(operation_transaction)
        print(f"All pending transaction : {self.pending_transaction}")
    def remove_pending_transaction(self, transaction_number):
        print(f"Transaction {transaction_number} is removed from pending queue")
        self.pending_transaction.remove(transaction_number)
        print(f"All pending transaction : {self.pending_transaction}")
    def run(self, schedule_list):
        for s in schedule_list:
            transaction_number = self.get_transaction_number(s)
            operation = self.get_operation(s)
            if(operation == 'R' or operation == 'W'):
                resource = self.get_resource(s)
            transaction = self.get_transaction(transaction_number= transaction_number)
            if(self.get_operation(s) == 'R' and transaction_number not in self.pending_transaction):
                if(self.is_acquired_by_transaction_number(resource=resource, access= 'shared', transaction_number=transaction_number) or self.is_acquired_by_transaction_number(resource=resource, access= 'exclusive', transaction_number=transaction_number)):
                    print(f"Transaction {transaction_number} read {resource}")
                    t_string = "R" + transaction_number + "(" + resource + ")"
                    self.final_schedule.append(t_string)
                elif(self.is_resource_free(resource= resource, transaction_number=transaction_number) or self.is_resource_only_shared_locked(resource=resource)):
                    access_type = self.get_access_type(resource=resource, transaction_number=transaction_number)
                    if(access_type == 'shared'):
                        transaction.add_shared_lock(resource = resource)
                        t_string = "Lock-S" + transaction_number + "(" + resource + ")"
                        self.final_schedule.append(t_string)
                    else:
                        transaction.add_exclusive_lock(resource = resource)
                        t_string = "Lock-X" + transaction_number + "(" + resource + ")"
                        self.final_schedule.append(t_string)

                    print(f"Transaction {transaction_number} read {resource}")
                    t_string = "R" + transaction_number + "(" + resource + ")"
                    self.final_schedule.append(t_string)
                else:
                    print(f"Transaction {transaction_number} still cant read on {resource} cos resource is not avail")
                    self.pending_transaction.add(transaction_number)
                    self.previous_operation_queue = self.operation_queue
                    transaction.add_operation_queue(s)
                    self.operation_queue.append(s)
            elif(self.get_operation(s) == 'W' and transaction_number not in self.pending_transaction):
                if(self.is_acquired_by_transaction_number(resource=resource, access= 'exclusive', transaction_number=transaction_number)):
                    print(f"Transaction {transaction_number} write {resource}")
                    t_string = "W" + transaction_number + "(" + resource + ")"
                    self.final_schedule.append(t_string)
                elif(self.is_resource_free(resource= resource, transaction_number=transaction_number)):
                    transaction.add_exclusive_lock(resource = resource)
                    t_string = "Lock-X" + transaction_number + "(" + resource + ")"
                    self.final_schedule.append(t_string)
                    print(f"Transaction {transaction_number} write {resource}")
                    t_string = "W" + transaction_number + "(" + resource + ")"
                    self.final_schedule.append(t_string)
                else:
                    print(f"Transaction {transaction_number} still cant write on {resource} cos resource is not avail")
                    self.add_pending_transaction(transaction_number)
                    transaction.add_operation_queue(s)
                    self.previous_operation_queue = self.operation_queue
                    self.operation_queue.append(s)
            elif(transaction_number in self.pending_transaction):
                    # self.add_pending_transaction(transaction_number)
                    print(f"Transaction {transaction_number} still cant commit on {resource} cos resource is not avail")
                    transaction.add_operation_queue(s)
                    self.previous_operation_queue = self.operation_queue
                    self.operation_queue.append(s)
            else:
                print(f"Transaction {transaction_number} commit")
                unlock_list = transaction.unlock_all_resources()
                self.final_schedule.extend(unlock_list)
                t_string = "C" + transaction_number
                self.final_schedule.append(t_string)

                for p in list(self.pending_transaction):
                    not_pending = True
                    t = self.get_transaction(transaction_number=p)
                    for o in list(t.operation_queue):
                        transaction_number = self.get_transaction_number(o)
                        operation = self.get_operation(o)
                        if(o[0] == 'C'):
                            resource = ''
                        else:
                            resource = self.get_resource(o)
                        transaction = self.get_transaction(transaction_number= transaction_number)
                        if(operation == 'R'):
                            if(self.is_acquired_by_transaction_number(resource=resource, access= 'shared', transaction_number=transaction_number) or self.is_acquired_by_transaction_number(resource=resource, access= 'exclusive', transaction_number=transaction_number)):
                                print(f"Transaction {transaction_number} read {resource}")
                                t_string = "R" + transaction_number + "(" + resource + ")"
                                self.final_schedule.append(t_string)
                                t.operation_queue.pop(0)
                                self.previous_operation_queue = self.operation_queue
                                self.operation_queue.remove(o)
                            elif(self.is_resource_free(resource= resource, transaction_number=transaction_number) or self.is_resource_only_shared_locked(resource=resource)):
                                access_type = self.get_access_type(resource=resource, transaction_number=transaction_number)
                                if(access_type == 'shared'):
                                    transaction.add_shared_lock(resource = resource)
                                    t_string = "Lock-S" + transaction_number + "(" + resource + ")"
                                    self.final_schedule.append(t_string)
                                else:
                                    transaction.add_exclusive_lock(resource = resource)
                                    t_string = "Lock-X" + transaction_number + "(" + resource + ")"
                                    self.final_schedule.append(t_string)
                                print(f"Transaction {transaction_number} read {resource}")
                                t_string = "R" + transaction_number + "(" + resource + ")"
                                self.final_schedule.append(t_string)
                                t.operation_queue.pop(0)
                                self.previous_operation_queue = self.operation_queue
                                self.operation_queue.remove(o)
                            else:
                                print(f"Transaction {transaction_number} still cant read on {resource} cos resource is not avail")
                                self.previous_operation_queue = self.operation_queue
                                not_pending = False
                                break
                        elif(operation == 'W'):
                            if(self.is_acquired_by_transaction_number(resource=resource, access= 'exclusive', transaction_number=transaction_number)):
                                print(f"Transaction {transaction_number} write {resource}")
                                t_string = "W" + transaction_number + "(" + resource + ")"
                                self.final_schedule.append(t_string)
                                t.operation_queue.pop(0)
                                self.previous_operation_queue = self.operation_queue
                                self.operation_queue.remove(o)
                            elif(self.is_resource_free(resource= resource, transaction_number=transaction_number)):
                                transaction.add_exclusive_lock(resource = resource)
                                t_string = "Lock-X" + transaction_number + "(" + resource + ")"
                                self.final_schedule.append(t_string)
                                print(f"Transaction {transaction_number} write {resource}")
                                t_string = "W" + transaction_number + "(" + resource + ")"
                                self.final_schedule.append(t_string)
                                t.operation_queue.pop(0)
                                self.previous_operation_queue = self.operation_queue
                                self.operation_queue.remove(o)
                            else:
                                print(f"Transaction {transaction_number} still cant write on {resource} cos resource is not avail")
                                self.previous_operation_queue = self.operation_queue
                                not_pending = False
                                break
                        else:
                            print(f"Transaction {transaction_number} commit")
                            unlock_list = transaction.unlock_all_resources()
                            self.final_schedule.extend(unlock_list)
                            t_string = "C" + transaction_number
                            self.final_schedule.append(t_string)
                            transaction.unlock_all_resources()
                                                
                            self.previous_operation_queue = self.operation_queue
                            self.operation_queue.remove(o)
                    if(not_pending):
                        self.remove_pending_transaction(p)
                            

            print("\n")
        while(len(self.operation_queue) > 0):
            if(self.previous_operation_queue == self.operation_queue):
                self.operation_queue[-1]
                print("Deadlock detected!")
                # break
                print(f"Transaction {self.get_transaction_number(self.operation_queue[0])} is rolled back")
                self.abort_transaction(transaction_number= self.get_transaction_number(self.operation_queue[0]))
                # break
            self.run(list(self.schedule_list))
            
        
        # print(f"Sisa: {self.operation_queue}")
        # print(f"Pending-transaction : {self.pending_transaction}")
        # print(f"PRevious: {self.previous_operation_queue}")
        # print(f"Schedule : {self.schedule_list}")

user_input = input("Enter schedule_list: ")
schedule_list = user_input.replace(" ", "").split(";")
schedule = Schedule(schedule_list=schedule_list)
schedule.run(schedule_list)
print(f"Final schedule : {schedule.final_schedule}")
