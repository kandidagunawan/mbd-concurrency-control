# mbd-concurrency-control

## Description

This is a project for the course "Database Management" at Bandung Institute of Technology. The project is about implementing a concurrency control protocol for a database management system. There are 2 protocols implemented in this project: Two-Phase Locking Protocol (2PL) and Optimistic Concurrency Control (OCC)
## Requirements

- Python 3

## How to run

### Two-Phase Locking Protocol

1. Clone this repository
2. Run the following command to run `Two-Phase Locking Protocol`

   ```
   python two_phase_locking.py
   ```
   or run the following command to run `Optimistic Concurrency Control Protocol`
   ```
   python occ.py
   ```

3. Enter the schedule you want to process with this format:
   ```

   R1(X); R2(Y); R1(Y); R2(X); C1; C2;

   ```
   Notes:
   Character inside the parentheses: resource
   R : Read
   W : Write
   Number: transaction_number
   C: Commit
   There's no input validation, so you have to follow this format!
   


