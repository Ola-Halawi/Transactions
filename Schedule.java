
/** @ author Ola Halawi

 CSC 621: Transactions Processing Systems
 Spring 2017-2018

This program simulates a scheduler that checks whether a particular schedule is serializable, and:
1.	Recoverable
2.	ACA
3.	Strict
4.	Rigorous
In additions to reads and writes, this schedule handles operations such as increment and decrement.
For equivalency, this scheduler considers conflict equivalence and not view equivalence.
the transactions should be complete, i.e., begin with a start operation and end with either a commit or abort operation.(When a transaction commits, it will not be able to send any operations. 
and when a transaction aborts, all of its effects should be removed)
the scheduler will produce an error message if the schedule is not serializable and it will exit. 

1. To check for conflict serializability: we construct a directed graph, called a serialization graph of the schedule, We define two operations to conflict if they operate on the same data DataItem and either at least one of them is a Write,
 										 or one is a Read and the other is an Increment or Decrement. 
										The vertices of the graph are the transactions in T that are committed in the schedule 
										the set of edges consists of all edges Ti--> Tj for which one of the following conditions holds:

											- Ti executes write(q) before Tj executes read(q) 
											- Ti executes read(q) before Tj executes write(q)
											- Ti executes write(q) before Tj executes write(q)
											- Ti executes write(q) before Tj executes Increment(q)
											- Ti executes write(q) before Tj executes Decrement(q)
											- Ti executes read(q) before Tj executes Increment(q)
											- Ti executes read(q) before Tj executes Decrement(q)
											- Ti executes Increment(q) before Tj executes write(q)
											- Ti executes Increment(q) before Tj executes read(q)
											- Ti executes Decrement(q) before Tj executes write(q)
											- Ti executes Decrement(q) before Tj executes read(q)

										If the serialization graph for a schedule S has a cycle, then S is not conflict serializable and an error message will be produced
										If the graph contains no cycle, then the schedule is conflict serializable

2. Recoverability: If Tj reads a data item previously written by Ti and the commit operation of Ti appears before the commit of Tj then the schedule is recoverable
3. ACA: In a cascadeless schedule, if Tj reads a data item previously written by Ti, then Ti commits before the read opertaion of Tj (no dirty reads)
4. Strict: Recoverable+ ACA+ if Tj writes a data item that is previously written by Ti then Ti commits before the write operation of Tj
5. Rigorous: Strict and and delays Write(x) until all transactions that previously Read(x) Commit/Abort
6. Increment: i1(x) means T1 wants to add 1 to data item x 
7. Decrement: d1(x) means T1 wants to subtract 1 from data item x 
 */

import java.util.*;

public class Schedule {

	LinkedList<Op> theSchedule;

	/*
	 * Default constructor
	 */

	Schedule(String[] schedule) {

		try {

			theSchedule = new LinkedList<Op>();

			fill(schedule);

		} 

		catch (Exception excep) {

			System.out.println("There is an error in writing the schedule");

			excep.printStackTrace();

		}

		/*
		 * Detecting if a transaction is committed, once a transaction commits it cxan
		 * not issue anymore transactions
		 */

		for (int i = 0; i < theSchedule.size(); i++) {

			Op operation = theSchedule.get(i);

			check:if (operation.getAction() == 'c') {

				int nbr = operation.getTransaction();

				for (int j = i+1 ; j < theSchedule.size(); j++) {

					Op operation2= theSchedule.get(j);

					if (operation2.getTransaction() == nbr) {

						System.out.println("Error: T" + nbr + " is committed, so it can not issue operations anymore. ");

						break check;

					}

				}

			}
		}


		/*
		 * Detecting if a transaction is aborted, once a transaction aborts it can not
		 * issue any more transcations and its effects should be removed from the
		 * schedule.
		 */

		for (int i = 0; i < theSchedule.size(); i++) {

			Op operation = theSchedule.get(i);

			if (operation.getAction() == 'a') {

				int nbr = operation.getTransaction();

				for (int j = i + 1; j <= theSchedule.size(); j++) {

					if (operation.getTransaction() == nbr) {

						System.out.println("Error: T" + nbr + " is aborted, so it can not issue any operations. ");

					}
				}


				for (int k = i; k >= 0; k--) {

					if (theSchedule.get(k).getTransaction() == nbr) {

						theSchedule.remove(k);


					}

				}

			}

		}
	}


	/*
	 * Method is used by the constructor to parse the array and invoke attach()
	 * method
	 * @param an array of strings
	 */

	private void fill(String[] schedule) {

		for (int i = 0; i < schedule.length; i++) {

			char action = schedule[i].charAt(0); // the action can be r,w,c,a,i,d

			int transaction = Integer.parseInt(schedule[i].substring(1,2));

			if (action=='c') { // a commit operation

				attachC('c', transaction);

			}

			else if (action=='a') { // an abort operation
				attachA('a',transaction);
			}

			else {

				char openParenthesis = schedule[i].charAt(2);

				char DataItem = schedule[i].charAt(3);

				char closeParenthesis = schedule[i].charAt(4);

				if (action=='r' || action=='w' ||action=='i'||action=='d') {

					attach(action, transaction, openParenthesis, DataItem, closeParenthesis);

				} 

			}
		}
	} 



	/*
	 * This method will attach a commit operation to the schedule
	 * @param char c, int transaction
	 */

	private void attachC(char c, int transaction) {

		theSchedule.add(new Op('c', transaction));

	}


	/*
	 * This method will attach an abort operation to the schedule
	 * @param char a , int transaction
	 */

	private void attachA(char a, int transaction) {

		theSchedule.add(new Op('a', transaction));

	}

	/*
	 * This method will attach a read, write, increment or decrement operations to the schedule
	 * @param char action, int transaction, char openParenthesis, char DataItem,
			char closeParenthesis
	 */

	private void attach(char action, int transaction, char openParenthesis, char DataItem,
			char closeParenthesis) {

		theSchedule.add(new Op(action, transaction, openParenthesis, DataItem, closeParenthesis));

	}

	/*
	 * 
	 * Method which returns a data item from the linkedlist theSchedule by the given
	 * index
	 * @param int index
	 * 
	 */

	public Op getDataItem(int index) {

		return theSchedule.get(index);

	}

	/*
	 * 
	 * @return a string representation of the full schedule
	 * 
	 */

	public String getSchedule() {

		String schedule = "";

		for(int i=0;i<theSchedule.size()-1;i++) {

			Op s= theSchedule.get(i);

			schedule += s + ",";

		}

		schedule+=theSchedule.getLast();

		return schedule;

	}


	/*
	 * Increment or decrement a data item by 1
	 * @return String r that declares which data item was incremented/decremented and by which transaction
	 */

	public String incDec() {

		// incrementing a data item x by 1

		String r="";

		for(int i=0; i<theSchedule.size();i++) {

			if(theSchedule.get(i).getAction()=='i') {


				r+="The data item "+ theSchedule.get(i).DataItem + " was incremented by 1 by T"+theSchedule.get(i).getTransaction()+"\n";
			}



			// decrementing a data item x by 1 


			if(theSchedule.get(i).getAction()=='d') {


				r+="The data item "+ theSchedule.get(i).DataItem + " was decremented by 1 by T"+theSchedule.get(i).getTransaction()+"\n";

			}

		}

		return r;

	}


	/*
	 * Method which evaluates a given schedule and explains whether it's
	 * conflict/not conflict serializable
	 * and where a cycle is found, iff is found.
	 * @return String result which says True if the schadule is conflict serializable and False otherwise
	 * 
	 */

	public String conflictSr() {

		String result = "Is Schedule Conflict-Serializable: ";

		HashSet<conflict> conflicts = test();

		for (conflict i : conflicts) {

			Op outerFrom = i.getFromOp();

			Op outerTo = i.getToOp();

			for (conflict j : conflicts) {

				Op innerFrom = j.getFromOp();

				Op innerTo = j.getToOp();

				check:	if (outerFrom.equals(innerTo) && outerTo.equals(innerFrom)) { // there is a cycle

					result += "False\n";

					result += "Error: There is a cycle between transactions: T" + outerFrom.getTransaction() + " and T"
							+ innerFrom.getTransaction();

					result+="\n";

					break check;
				}

				System.out.println(result);

			}

		}

		result += "True\n";

		result += "The Serialization Graph is acyclic, thus the schedule is conflict serializable.\n"; // there is no cycle, so the schedule is conflict serializable

		result+="\n";

		return result;

	}


	boolean re = true; // true if the schedule is recoverable and false otherwise
	boolean aca = true; // true if the schedule is ACA and false otherwise
	boolean st = true; // true if the schedule is strict and false otherwise

	/*
	 * 
	 * Method which evaluates a given schedule and tells whether it is recoverable
	 * or not
	 * @return String result that is True if the schedule is recoverable and False otherwise
	 */

	public String recoverable() {

		int t1=0;

		int t2=0;

		String result = "Is Schedule recoverable: ";

		for (int i = 0; i < theSchedule.size(); i++) {

			Op outerOp = theSchedule.get(i);

			for (int j = 0; j < theSchedule.size(); j++) {

				Op innerOp = theSchedule.get(j);

				// it's the same transaction, so no conflicts

				if ((outerOp.getTransaction() == innerOp.getTransaction())) {

					continue;

					// both are Reading, so there are no conflicts

				}

				else if (outerOp.getAction() == 'r' && innerOp.getAction() == 'r') {

					continue;

				}

				// transactions are operating on different data items so no conflicts

				else if (outerOp.getDataItem() != innerOp.getDataItem()) {

					continue;

				}

				// a read before write, does not affect recoverability

				else if (outerOp.getAction() == 'r' && innerOp.getAction() == 'w') {

					continue;

				}

				// write conflicts do not affect recoverability

				else if (outerOp.getAction() == 'w' && innerOp.getAction() == 'w') {

					continue;

				}

				// w-r conflict

				else {

					if (outerOp.getAction() == 'w' && innerOp.getAction() == 'r') {

						int nbr1 = outerOp.getTransaction();

						int nbr2 = innerOp.getTransaction();


						for (int m = 0; m < theSchedule.size(); m++) {

							for(int n=0;n<theSchedule.size();n++) {

								if (theSchedule.get(m).getAction() == 'c'
										&& theSchedule.get(m).getTransaction() == nbr1) { //the transaction that wrote the data item committed

									t1-=m;


								}

								if (theSchedule.get(n).getAction() == 'c'
										&& theSchedule.get(n).getTransaction() == nbr2) { // the transaction that read the data item committed

									t2-=n;

								}


							}

						}

						//comparing which transaction committed first

						if (t1>t2 && t1<0 && t2<0) {

							result+="True\n";

							re=true;

							return result;
						}

						else {

							result+="False\n";

							re=false;

							return result;

						}

					}

				}
			}
		}

		result+="True\n";

		re=true;

		return result;

	}


	/*
	 * @return String result that is True if the schedule is ACA and False otherwise
	 */

	public String ACA() {

		String result = "Is Schedule Cascadeless: ";

		for (int i = 0; i < theSchedule.size(); i++) {

			Op outerOp = theSchedule.get(i);

			int nbr1=theSchedule.get(i).getTransaction();

			for (int j = 0; j < theSchedule.size(); j++) {

				Op innerOp = theSchedule.get(j);


				// it's the same transaction, so no conflicts

				if ((outerOp.getTransaction() == innerOp.getTransaction())) {

					continue;

					// both are Reading, so there are no conflicts

				}

				else if (outerOp.getAction() == 'r' && innerOp.getAction() == 'r') {

					continue;

				}

				// transactions are operating on different data items so no conflicts

				else if (outerOp.getDataItem() != innerOp.getDataItem()) {

					continue;

				}

				// a read before write, does not affect the schedule being ACA

				else if (outerOp.getAction() == 'r' && innerOp.getAction() == 'w') {

					continue;

				}

				// write conflicts do not affect ACA

				else if (outerOp.getAction() == 'w' && innerOp.getAction() == 'w') {

					continue;

				}

				// w-r conflict

				else {

					if (outerOp.getAction() == 'w' && innerOp.getAction() == 'r') {

						for (int m = i; m < j; m++) {

							if (theSchedule.get(m).getAction() == 'c' && theSchedule.get(m).getTransaction()==nbr1) {

								result += "True\n"; // the transaction that wrote the data item committed before the other transaction read that data item

								aca=true;

								return result;

							}


						}

						result += "False\n"; 

						aca=false;

						return result;

					}

				}

			}

		}

		result += "True\n";

		aca=true;

		return result;

	}



	/*
	 * This method checks if a schedule is strict or not
	 * @return String result that is True if the schedule is strict and False otherwise
	 */

	public String strict() {

		String result = "Is Schedule Strict: "; 

		for (int i = 0; i < theSchedule.size(); i++) {

			Op outerOp = theSchedule.get(i);

			int nbr1= outerOp.getTransaction();

			for (int j = 0; j < theSchedule.size(); j++) {

				Op innerOp = theSchedule.get(j);

				int nbr2= innerOp.getTransaction();

				// it's the same transaction, so no conflicts

				if ((outerOp.getTransaction() == innerOp.getTransaction())) {

					continue;

					// both are Reading, so there are no conflicts

				}

				else if (outerOp.getAction() == 'r' && innerOp.getAction() == 'r') {

					continue;

				}

				// transactions are operating on different data items so no conflicts

				else if (outerOp.getDataItem() != innerOp.getDataItem()) {

					continue;

				}

				// a read before write, does not affect strictness

				else if (outerOp.getAction() == 'r' && innerOp.getAction() == 'w') {

					continue;

				}

				// write conflicts 
				//already checked for w-r conflicts in the ACA method

				if (aca==true) {

					if (theSchedule.get(i).getAction() == 'w' && theSchedule.get(j).getAction() == 'w'&& outerOp.getDataItem() == innerOp.getDataItem()
							&& nbr1!=nbr2) {

						for (int m = i+1; m < j; m++) {

							if (theSchedule.get(m).getAction() == 'c'&& theSchedule.get(m).getTransaction()==nbr1) {

								result+="True\n";

								st=true;

								return result;
							}

						}

						result+="False\n";

						st=false;

						return result;

					}


					result+="True\n";

					st=true;

					return result;

				}

			}
		}


		if(aca==false){

			result+="False\n";

			st=false;


		}

		else {

			result+="True\n";

			st=true;
		}

		return result;

	}

	/*
	 * @return String result that is True if the schedule is rigorous and False otherwise
	 */

	public String rigorous() {

		String result = "Is Schedule Rigorous: ";

		for (int i = 0; i < theSchedule.size(); i++) {

			Op outerOp = theSchedule.get(i);

			for (int j = 0; j < theSchedule.size(); j++) {

				Op innerOp = theSchedule.get(j);

				// it's the same transaction, so no conflicts

				if ((outerOp.getTransaction() == innerOp.getTransaction())) {

					continue;

					// both are Reading, so there are no conflicts

				}

				else if (outerOp.getAction() == 'r' && innerOp.getAction() == 'r') {

					continue;

				}

				// transactions are operating on different data items so no conflicts

				else if (outerOp.getDataItem() != innerOp.getDataItem()) {

					continue;

				}

				// a read before write

				if (st==true) {

					if (outerOp.getAction() == 'r' && innerOp.getAction() == 'w' && outerOp.getTransaction()!=innerOp.getTransaction()
							&& outerOp.getDataItem()==innerOp.getDataItem()) {


						for (int m = i; m < j; m++) {

							if (theSchedule.get(m).getAction() == 'c'&& theSchedule.get(m).getTransaction()==outerOp.getTransaction()) {

								result+="True\n";

								return result;


							}

						}

						result+="False\n";

						return result;

					}

					result+="True\n";
					return result;

				}


			}
		}

		if(st==false) {

			result+="False\n";

		}

		else {

			result+="True\n";

		}

		return result;		

	}


	/*
	 * 
	 * @return a hashset of type conflict containing pairs of nodes that conflict in the given schedule
	 * 
	 */

	private HashSet<conflict> test() {

		HashSet<conflict> results = new HashSet<conflict>();

		for (int i = 0; i < theSchedule.size(); i++) {

			Op outerOp = theSchedule.get(i);

			for (int j = 0; j < theSchedule.size(); j++) {

				Op innerOp = theSchedule.get(j);

				// it's the same transaction, so no conflicts

				if ((outerOp.getTransaction() == innerOp.getTransaction())) {

					continue;

				}

				// both are Reading, so there are no conflicts

				else if (outerOp.getAction() == 'r' && innerOp.getAction() == 'r') {

					continue;

				}

				// transactions are operating on different data items so no conflicts

				else if (outerOp.getDataItem() != innerOp.getDataItem()) {

					continue;

				}

				// both are incrementing, so there are no conflicts

				else if (outerOp.getAction() == 'i' && innerOp.getAction() == 'i') {

					continue;

				}

				// both are decrementing, so there are no conflicts

				else if (outerOp.getAction() == 'd' && innerOp.getAction() == 'd') {

					continue;

				}

				// one is incrementing and the othere is decrementing so no conflicts

				else if (outerOp.getAction() == 'i' && innerOp.getAction() == 'd') {

					continue;

				}

				// one is incrementing and the othere is decrementing so no conflicts

				else if (outerOp.getAction() == 'd' && innerOp.getAction() == 'i') {

					continue;

				}

				//both are commiting, no conflicts

				else if (outerOp.getAction() == 'c' && innerOp.getAction() == 'c') {

					continue;

				}

				// conflict

				else {

					// if outer Op is before inner Op

					if (i < j) {

						results.add(new conflict(outerOp, innerOp));

					}

					else if (i > j) {

						results.add(new conflict(innerOp, outerOp));

					} else {

						System.out.println("Shouldn't be printer.");

					}

				}

			}

		}

		return results;

	}

}



/*
 * 
 * Op is a class which stores the individual Ops of a given schedule.
 * 
 * Each Op object stores the action, transaction and the DataItem e.g. r2w.
 * 
 * A schedule is simply broken down into individual Ops.
 * 
 */

class Op {

	char action; // A variable which holds the action, it could be r,w,i,d,c or a

	int transaction; // A variable which holds the transaction number

	char openParenthesis;

	char DataItem; // A variable which holds the DataItem number

	char closeParenthesis;



	/*
	 * operation constructor
	 */

	Op(char action, int transaction, char openParenthesis, char DataItem, char closeParenthesis) {

		this.action = action; // convert string to char

		this.transaction = transaction;

		this.openParenthesis = openParenthesis;

		this.DataItem = DataItem; // convert string to char

		this.closeParenthesis = closeParenthesis;

	}

	/*
	 * operations Constructor used for commit/abort operations
	 */

	public Op(char action, int transaction) {
		this.action = action;
		this.transaction = transaction;

	}

	/*
	 * @return the action of an operation
	 */

	public char getAction() {

		return action;

	}

	/*
	 * @return the number of the transaction in an operation
	 */

	public int getTransaction() {

		return transaction;

	}

	/*
	 * @return the data item of an operation
	 */

	public char getDataItem() {

		return DataItem;

	}

	/*
	 *@return an operation in the form of a string
	 */

	public String toString() {

		return String.valueOf(action) + transaction + openParenthesis + String.valueOf(DataItem) + closeParenthesis;

	}

	/*
	 * This method compares transaction
	 * since we want the conflicting operations to be unique based on transaction
	 */

	public boolean equals(Object obj) {

		if (this == obj)

			return true;

		if (obj == null)

			return false;

		if (getClass() != obj.getClass())

			return false;

		Op other = (Op) obj;

		if (transaction != other.transaction)

			return false;

		return true;

	}

}

// the class is used to manage a conflict in 2 Operations

class conflict {

	Op fromOp; // A variable storing an Op object 

	Op toOp; // A variable storing an Op object 

	conflict(Op from, Op to) {

		this.fromOp = from;

		this.toOp = to;

	}

	/*
	 * 
	 * @return the from conflicting operation
	 * 
	 */

	public Op getFromOp() {

		return fromOp;

	}

	/*
	 * 
	 * Method which returns the to conflicting operation
	 * 
	 */

	public Op getToOp() {

		return toOp;

	}

	/*
	 * Used by schedule class to see if there are any equal pairs in the set in the
	 * same order.
	 */

	public boolean equals(Object obj) {

		if (this == obj)

			return true;

		if (obj == null)

			return false;

		if (getClass() != obj.getClass())

			return false;

		conflict other = (conflict) obj;

		if (fromOp == null) {

			if (other.fromOp != null)

				return false;

		} else if (!fromOp.equals(other.fromOp))

			return false;

		if (toOp == null) {

			if (other.toOp != null)

				return false;

		} else if (!toOp.equals(other.toOp))

			return false;

		return true;

	}

}
