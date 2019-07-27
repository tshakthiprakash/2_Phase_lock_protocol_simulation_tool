1.Open command prompt and change the directory to the project folder
2.The schedule to be executed must be in the input.txt which must be in the same project folder
3.Each operation in the Schedule must be entered in a new line and there must be a space after a transaction id in each operations ie, r1 (X)
4.To execute the program, type python Rigorous_2PL_wound_wait.py
5.The output for each operation can be seen in the command line
6.Final transaction table and the lock table are displayed

Sample input:
---------------
b1;
r1 (Y);
w1 (Y);
r1 (Z);
b2;
r2 (Y);
b3;
r3 (Z);
w1 (Z);
w2 (Y);
r2 (X);
e1;
w3 (Z);	
e3;
w2 (X);
e2;