/**
 * 
 * ScheduleRun class used to launch the application
 * 
 * @author Ola Halawi
 * 
 */


import javax.swing.*;

import java.awt.event.ActionEvent;

import java.awt.event.ActionListener;

import java.awt.Color;

import java.awt.Font;


public class ScheduleRun {

	public static void main(String[] args) {

		//Schedule a job for the event-dispatching thread:
		//creating and showing this application's GUI.

		javax.swing.SwingUtilities.invokeLater(new Runnable() {

			public void run() {

				createAndShowGUI(); 
			}
		});
	}

	public static void createAndShowGUI() {

		//Create and set up the window.

		JFrame frame = new JFrame("Scheduler Checker");

		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);


		//Create and set up the content pane.

		AppDisplay newContentPane = new AppDisplay();

		newContentPane.setOpaque(true); 

		frame.setContentPane(newContentPane);


		//Display the window.

		frame.pack();

		frame.setSize(1200, 1000);

		frame.setVisible(true);

	}


}


class AppDisplay extends JPanel
implements ActionListener {

	private JLabel descriptionLabel;

	private JTextField userInput;

	private JLabel userInputLabel;

	private JTextArea textArea;

	private JLabel resultLabel;


	public AppDisplay() {
		setLayout(null);


		JButton btnSubmit = new JButton("Submit");

		btnSubmit.setFont(new Font("Serif", Font.BOLD, 18));

		JButton btnReset= new JButton("Reset");

		btnReset.setFont(new Font("Serif", Font.BOLD, 18));


		btnSubmit.addActionListener(new ActionListener() {

			@SuppressWarnings("unused")
			public void actionPerformed(ActionEvent e) {

				Schedule s;

				//grab user input
				String userData = userInput.getText();

				//String[] schedule = { "w1(x)", "w1(y)", "r2(u)", "w1(z)", "c1","w2(x)", "r2(y)","w2(y)","c2"};
				//String[] schedule = { "w1(x)", "w1(y)", "r2(u)", "w2(x)","r2(y)", "c2","w1(z)","c1"};
				//String[] schedule = { "w1(x)", "w1(y)", "r2(u)", "w2(x)","r2(y)","w2(y)", "w1(z)","c1","c2"};
				//String[] schedule = { "w1(x)","w1(y)","r2(u)","w2(x)","w1(z)","c1","r2(y)","w2(y)","c2"};


				//strip the content from any spaces and double quotes as those would break the schedule function

				String[] schedule = userData.replaceAll("\\s","").replaceAll("\"", "").split(",");

				// System.out.println(userData);

				s = new Schedule(schedule);

				String result = new String();

				result = "The schedule: " + s.getSchedule() + "\n";

				result+="\n";

				result += "";

				result += s.conflictSr();
				
				result+="\n";

				result += "";
				
				result+= s.incDec();
				
				result+="\n";

				result += (s.recoverable());

				result += (s.ACA());

				result += (s.strict());

				result += (s.rigorous());

				result+="\n";

				textArea.setText(result);

			}
		});


		btnSubmit.setBounds(610, 220, 200, 30);

		add(btnSubmit);


		//Description of the program

		descriptionLabel = new JLabel("<html>This program simulates a scheduler that checks whether a particular schedule is conflict serializable, and:<br/>" + 
				"1.	Recoverable<br/>" + 
				"2.	Cascadelsess<br/>" + 
				"3.	Strict<br/>" + 
				"4.	Rigorous<br/>" + 
				"In additions to reads and writes, this p handles operations such as increment and decrement.</html>", null, JLabel.CENTER);

		descriptionLabel.setBounds(0, 0, 1000, 200);

		add(descriptionLabel);		

		descriptionLabel.setFont(new Font("Serif", Font.BOLD, 18));

		descriptionLabel.setForeground(Color.BLUE);


		userInputLabel = new JLabel("Enter Schedule Here");

		userInputLabel.setBounds(50, 220, 300, 20);

		userInputLabel.setFont(new Font("Serif", Font.BOLD, 16));

		add(userInputLabel);


		//a text field where the user can enter his input

		userInput = new JTextField();

		userInput.setBounds(200, 220, 390, 30);

		userInput.setFont(new Font("Serif", Font.PLAIN, 22));

		add(userInput);

		userInput.setColumns(10);


		// a window to display the output 

		resultLabel = new JLabel("Output");

		resultLabel.setFont(new Font("Serif", Font.BOLD, 24));

		resultLabel.setForeground(Color.RED);

		resultLabel.setBounds(50, 500, 400, 40);

		add(resultLabel);


		textArea = new JTextArea();

		textArea.setBounds(200, 300, 800, 500);

		textArea.setFont(new Font("Serif", Font.PLAIN, 22));

		add(textArea);



		btnReset.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {

				userInput.setText("");

				textArea.setText("");

			}});

		btnReset.setBounds(830, 220, 200, 30);

		add(btnReset);
	}




	public void actionPerformed(ActionEvent e) {

	}



	public static void main(String[] args) {

	}
}





