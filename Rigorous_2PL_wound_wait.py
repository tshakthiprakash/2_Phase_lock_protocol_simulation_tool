'''
This project that simulates the rigorous 2_Phase_locking with wound-wait protocol is done as a part of CSE 5331-001 Summer 2019
Authors :
Shakthi Prakash Thiruvathilingam (1001518112)

#Reference Source:
#https://stackoverflow.com/questions/16333296/how-do-you-create-nested-dict-in-python
#https://docs.python.org/2/tutorial/inputoutput.html
'''
import sys;
#Transaction table is nested dictionaries where the key is the transaction ID and the values are the transaction attributes
transaction_table={}
# Lock table is a nested dictionaries where the key is the item ID and values are the item attributed
lock_table={}
# Timestamp is instantiated to 0(Transaction with lesser timestamp is the older) 
ts = 0	 

def read_item(line):
	#reads the operation from the line ie, r in r1 (X)
	operation = line[0] 
	line_transaction = line.split(" ")
	# reads the transaction id from the line ie, 1 in r1 (X)
	transaction_id = line_transaction[0][1]
	#reads the item id from the line ie, X from r1 (X)
	item_id = line_transaction[1][1] 
	status_RL = "read_lock"
	#Transaction record is extracted from the transaction table using the transaction_id
	current_transaction = transaction_table[transaction_id]
	
	#Phaseof the transaction is true when it is in expanding phase
	if current_transaction['phase'] == True:
		# Check if there is entry of item id in the lock table
		if item_id in lock_table: 
			# Extract the item record from the lock table using the item id
			current_lock = lock_table[item_id]
			#check if lock status on the item record is read lock
			if current_lock['lock_status'] == "read_lock":
				#check if the current transaction is holding the read lock
				if transaction_id not in current_lock['locking_transactions']:
					# if the transaction is not holding the lock add the transaction id to the list of transaction holding the read lock in the lock table
					(current_lock['locking_transactions']).append(transaction_id)
					current_transaction['trans_item'].append(item_id)
					print("\nExecuting " + line + "\nChanges in Tables: The item " + str(item_id)+ "'s lock status for the transaction "  + str(transaction_id) + " is set to Read lock in the lock table")
			
			#check if lock status on the item record is write lock
			elif current_lock['lock_status'] == "write_lock":
				if len(current_lock['locking_transactions'])==0:
					current_lock['lock_status'] = status_RL
					if item_id not in current_transaction['trans_item']:
						current_transaction['trans_item'].append(item_id)
					print("\nExecuting " + line + "\nChanges in Tables: The item " + str(item_id) + "'s lock status for the transaction " + str(transaction_id) + " is downgraded from 'Write lock' to 'Read lock' in the lock table")
				
				#check if the current transaction is holding the write lock
				elif transaction_id not in current_lock['locking_transactions']:
					# some other transaction holding the write lock so we have to call the wound-wait to the resolve the conflict
					t = current_lock['locking_transactions'][0]
					woundwait(t,transaction_id,operation,item_id,line)
			
			#check if lock status on the item record is empty
			elif current_lock['lock_status'] == " ":
				if transaction_id not in current_lock['locking_transactions']: 
					(current_lock['locking_transactions']).append(transaction_id)
					current_transaction['trans_item'].append(item_id)
					current_lock['lock_status']=status_RL
					print("\nExecuting " + line + "\nChanges in Tables: The item " + str(item_id)+ "'s lock status for the transaction "  + str(transaction_id) + " is set to Read lock in the lock table")
		
		# if the item is not available in the lock table, then make new record in the lock table
		else: 
			lock_table[item_id] = {'lock_status': status_RL, 'locking_transactions': [transaction_id], 'list_of_transactions_waiting': []}
			transaction_table[transaction_id]['trans_item'].append(item_id)
			print("\nExecuting " + line + "\nChanges in Tables: Added item_id entry into lock table, Lock Status is set to 'Read Lock', The item " + str(item_id) + " for transaction " + str(transaction_id) +" is newly added into lock table and also added the item to transaction table in corresponding transaction")

def write_item(line):
	#reads the operation from the line ie, W in W1 (X)
	operation = line[0]
	line_transaction = line.split(" ")
	# reads the transaction id from the line ie, 1 in W1 (X))
	transaction_id = line_transaction[0][1]
	#reads the item id from the line ie, X in W1 (X)
	item_id = line_transaction[1][1]
	status_WL = "write_lock"
	#Transaction record is extracted from the transaction table using the transaction_id
	current_transaction = transaction_table[transaction_id]

	#Phaseof the transaction is true when it is in expanding phase.	
	if transaction_table[transaction_id]['phase'] == True:
		# Check if there is entry of item id in the lock table
		if item_id in lock_table:
			# Extract the item record from the lock table using the item id
			current_lock = lock_table[item_id]
			# Extract the list of transactions holding the item from the lock table.
			t_list = current_lock['locking_transactions']
			#check if lock status on the item record is read lock	
			if current_lock['lock_status'] == "read_lock":
				#check if the current transaction is holding the read lock on the item, if it is the case then the lock is upgraded to write lock.
				if (len(t_list)==1 and t_list[0] == transaction_id):
					current_lock['lock_status'] = "write_lock"
					print("\nExecuting " + line + "\nChanges in Tables: The item " + str(item_id) + "'s lock status for transaction "+ str(transaction_id) +" is upgraded from 'Read Lock' to 'Write Lock'")

				else:
					#check if the current transaction is already blocked, then the operation is added to the list of waiting operations for the transaction.
					if current_transaction['trans_state'] == "blocked":
						current_transaction['list_of_waitingoperations'].append({'operation':line, 'item_id':item_id})
						print("\nExecuting " + line + "\nTransaction " +str(transaction_id)+ "is already blocked, so this operation is will resume when the transaction " + str(transaction_id) + " unblocks.")
					#check if the current transaction is already aborted, the transaction remains aborted and the operations are discarded.
					elif current_transaction['trans_state'] == "aborted":
						print("\nExecuting " + line + "\nTransaction " +str(transaction_id)+ "is already aborted, so this operation is disregarded")
					#multiple read locks on the item, wound-wait is called to resolve the conflict
					else:
						print("\nExecuting " + line + "\nChanges in Tables: Found multiple read locks on the item " + str(item_id) + " already. The write item operation for this transaction " + str(transaction_id) + ". and the read locks by other transaction(s) are conflicting operations. The wound-wait protocol is activated to resolve the situation.")
						for t1 in current_lock['locking_transactions']:
							woundwait(t1,transaction_id,operation,item_id,line)#check the timestamp of the current transaction against all the transactions in the list so as to abort or terminate one among them using wait die protocol
			
			#check if lock status on the item record is write lock
			elif current_lock['lock_status'] == "write_lock":
				#check if the current transaction is holding the write lock
				if transaction_id not in current_lock['locking_transactions']:
					# some other transaction holding the write lock so we have to call the wound-wait to the resolve the conflict
					t = current_lock['locking_transactions'][0] 
					woundwait(t,transaction_id,operation,item_id,line)
			
			#check if lock status on the item record is empty
			elif current_lock['lock_status'] == " ":
				current_lock['lock_status'] = "write_lock"
				current_lock['locking_transactions'].append(transaction_id)
				transaction_table[transaction_id]['trans_item'].append(item_id)
				
				print("\nExecuting " + line + "\nChanges in Tables: The item " + str(item_id) + "'s lock status for transaction "+ str(transaction_id) +" is set to 'Write Lock'")
				
		# if the item is not available in the lock table, then make new record in the lock table
		else:
			lock_table[item_id] = {'lock_status': status_WL, 'locking_transactions': [transaction_id], 'list_of_transactions_waiting': []}
			transaction_table[transaction_id]['trans_item'].append(item_id)
			print("\nExecuting " + line + "\nChanges in Tables: Added item_id entry into lock table, Lock Status is set to 'Write Lock', The item " + str(item_id) + " for transaction " + str(transaction_id) +" is newly added into lock table and also added the item to transaction table in corresponding transaction")

'''In this scheme, if a transaction requests to lock a resource (data item), which is already held with a conflicting lock by another transaction, then one of the two possibilities may occur −

If TS(Ti) < TS(Tj) − that is Ti, which is requesting a conflicting lock, is older than Tj − then Tj is aborted and Ti is given access to the data-item

If TS(Ti) > TS(tj) − that is Ti is younger than Tj − then Ti waits till the lock on the data_item is released by Tj.'''

# function handle the wound-wait protocol
# t2 is the requesting transaction and t1 is the transaction that is holding the item
def woundwait(t1,t2,operation,item_id,line):
	current_lock = lock_table[item_id]
	t_list = lock_table[item_id]['locking_transactions']
	t_aborted = "aborted"
	t_blocked = "blocked"
	Trans1 = transaction_table[t1]
	Trans2= transaction_table[t2]
	TS1 = Trans1['Timestamp']
	TS2 = Trans2['Timestamp']

	if Trans1 == Trans2:
		return "Both Transactions are same"
	#check if the conflicting transactions are active inorder to implement wound-wait
	if Trans2['trans_state'] != t_blocked or Trans2['trans_state'] != t_aborted: 
		# If TS2 is older than TS1, TS1 is aborted and releases all the items that were held by it and all the operations of the transactions that are waiting for those items are resumed. 
		# T2 is provided with the item
		if TS2 < TS1:
			Trans2['list_of_waitingoperations'].append({'operation':line, 'item_id':item_id})
			current_lock['list_of_transactions_waiting'].append(t2)
			Trans1['trans_state'] = t_aborted
			print(" \nExecuting " + line + "Changes in Tables: Transaction "+ str(t1) + " is aborted. Also, transaction" + str(t2) + "has been provided with the item" + str(item_id) + " based on wound wait protocol.")
			end_transaction(t1,t_aborted,line)
			return "got_the_item"
		
		# If TS2 is younger than TS1, TS2 is made to wait and the operation is added to the list of waiting operations of the transaction t2
		else:
			# check if the operation is already in the list of waiting operations of the transaction
			if {'operation':line, 'item_id':item_id} not in Trans2['list_of_waitingoperations']:
				Trans2['trans_state'] = t_blocked
				current_lock['list_of_transactions_waiting'].append(t2)
				Trans2['list_of_waitingoperations'].append({'operation':line, 'item_id':item_id})
				print("\nExecuting " + line + "\nChanges in Tables: Transaction "+ str(t2) +" is blocked, and the operation " + operation +  "is added to list of waiting operations : " ,Trans2['list_of_waitingoperations'], "\nAlso, for the item " + item_id + " the transaction is added to the list of waiting transactions in the lock table : ",current_lock['list_of_transactions_waiting'], " based on wound wait protocol.")
				return "wait"

# function to handle the commit and abort of a transaction
#t is the transaction ID
#t_state is the transaction state i.e committed or aborted.
def end_transaction(t,t_state,line):
	try:
		status_RL = "read_lock"
		status_WL = "write_lock"
		t_active = "active"
		t_aborted = "aborted"
		t_committed = "committed"

		current_transaction = transaction_table[t]
		current_transaction['trans_state']=t_state
		#the phase is changed to false which is the shrinking phase.
		current_transaction['phase'] = False 
		#if the status of the transaction is committed
		if t_state == "committed" :
			current_transaction['trans_state']="committed"
			print("\nExecuting " + line + "\nChanges in Tables: Transaction " + str(t) + " is committed, and the status is updated to committed The items to be unlocked: " + str(transaction_table[t]['trans_item']))
		
		#iterate through all the items that are being held by the transaction
		for d_item in current_transaction['trans_item']:
			current_lock = lock_table[d_item]
			t_list = current_lock['locking_transactions']
			waiting_t_list = current_lock['list_of_transactions_waiting']	
			#check if there is only one transaction holding that item
			if len(t_list) == 1:
				current_lock['lock_status'] = " "
			#iterate through all the transactions holding that item	
			if t in t_list:
				#remove the transactions from the list of holding transactions in the item record
				t_list.remove(t)
			# check if there are no transactions waiting for that item	
			if len(waiting_t_list)== 0:
				continue 
			#resume the transaction which was waiting for that item	
			first_waiting_trans = waiting_t_list.pop(0)
			trans1 = transaction_table[first_waiting_trans]
			if current_transaction != trans1:
				trans1["trans_state"]="active"
			i=len(trans1['list_of_waitingoperations'])
			# executing all the waiting operations of the transaction
			while i>0: 
				op1 = trans1['list_of_waitingoperations'].pop(0) 
				if op1['operation'][0] == "r":
					read_item(op1['operation'])
				if op1['operation'][0] == "w":
					write_item(op1['operation'])
				if op1['operation'][0] == "e":
					end_transaction(op1['operation'][1],t_state,op1['operation'])
				i=i-1
		current_transaction['trans_item'].clear()

	except IndexError as e:
		e = sys.exc_info()[0]

#read the input file
with open("input.txt", "r") as file: 
	#iterate through the file, line by line
	for line in file:
		operation = line[0]
		# check if the operation is begin or end
		if operation == "b" or operation == "e":
			 transaction_id = line[1]
			 if operation == "b":
				 t_state = "active"
				 phase = True
				 #timestamp is incrementd by 1
				 ts = ts + 1 
				 transaction_table[transaction_id] = {'trans_state': t_state, 'Timestamp': ts, 'list_of_waitingoperations': [], 'trans_item' : [], 'phase': phase}
				 # New Record for the transaction is added to the transaction table
				 print("\nExecuting " +line+ "\nChanges in Tables: New transaction is added to the transaction table with Transaction ID: " + str(transaction_id))

			#check if the operation is end	 
			 elif operation == "e":
				 if transaction_id in transaction_table:
					#check if the current transaction is already blocked, then the operation is added to the list of waiting operations for the transaction.
					 if transaction_table[transaction_id]['trans_state'] == 'blocked': 
						 transaction_table[transaction_id]['list_of_waitingoperations'].append({'operation':line, 'item_id' : 'N.A.'})
						 print("\nExecuting " + line + "\nChanges in Tables: Since Transaction "+ str(transaction_id) + " is already blocked, e" + str(transaction_id) + " is added to list of waiting operations")
					
					#check if the current transaction is already aborted, the transaction remains aborted and the operations are discarded.		
					 elif transaction_table[transaction_id]['trans_state'] == 'aborted': 
						 print("\nExecuting " + line + "\nChanges in Tables: Transaction " + str(transaction_id) +" is already aborted, its current Transaction state remains: " + str(transaction_table[transaction_id]['trans_state']))

					 else:
						 commit_transaction = "committed"
						 end_transaction(transaction_id,commit_transaction,line) 
				 else:
					 print(str(line) + ": Not Executed" + "\nReason: You cannot end a transaction which never began")
		
		# check if the operation is read or write
		elif operation == "r" or operation == "w":
			line_transaction = line.split(" ")
			transaction_id = line_transaction[0][1]
			item_id = line_transaction[1][1]
			if operation == "r" :
				status = "read_lock"
			else :
				status = "write_lock"
			#check if the transaction is there in the transaction table
			if transaction_id in transaction_table:
				#check if the current transaction is already blocked, then the operation is added to the list of waiting operations for the transaction.
				if transaction_table[transaction_id]['trans_state'] == 'blocked': 
					print("\nExecuting " + line + "\nChanges in Tables: transaction " + str(transaction_id) + " is already blocked")
					transaction_table[transaction_id]['list_of_waitingoperations'].append({'operation':line, 'item_id':item_id})
					if item_id in lock_table:
						lock_table[item_id]['list_of_transactions_waiting'].append(transaction_id)
					else:
						lock_table[item_id] = {'lock_status': status, 'locking_transactions': [], 'list_of_transactions_waiting': [] }
						lock_table[item_id]['list_of_transactions_waiting'].append(transaction_id)
				
				#check if the current transaction is already aborted, the transaction remains aborted and the operations are discarded.		
				elif transaction_table[transaction_id]['trans_state'] == 'aborted':
					print("\nExecuting " + line + "\nChanges in Tables: transaction " + str(transaction_id) + " is already aborted, and the state remains " + str(transaction_table[transaction_id]['trans_state']) + " and no futher operations can take place for this transaction")
				#check if the transaction is active, then execute the corresponding operation
				elif transaction_table[transaction_id]['trans_state'] == 'active':
					if operation == "r":
						read_item(line)
					if operation == "w":
						write_item(line)

			else:
				print(str(line) + ": Not Executed" + "\nReason: You cannot perform read/write operations for a transaction which never began")

# Display the Transaction table and Lock Table
print()
print("Transaction Table:")
print()
print(transaction_table)
print()
print("Lock Table:")
print()
print(lock_table)
