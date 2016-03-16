package simpledb.skyline.bnl;

import java.awt.BorderLayout;
import java.awt.Color;

import javax.swing.BorderFactory;
import javax.swing.JFrame;
import javax.swing.JPanel;

public class GraphicTest extends JFrame{


	 
	 public GraphicTest(){
		pack();
		setLocationByPlatform(true);
		 setSize(300,350);
		// pack();
		 setLocationRelativeTo(null);
		final GraphicPresentation display = new GraphicPresentation(getSize());
		 add(display);
		 
		 setTitle("Points");
		 setVisible(true);
	   //  pack();
//	     setSize(300,300);
	     // setLocation(450, 50);
	      //  setBackground(Color.RED);
	        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	      //  setBorder(BorderFactory.createLineBorder(Color.GRAY,3));
	      //  int cellSize = 600/GRID_SIZE; // Aim for about a 600-by-600 pixel board.
	        
	       
	        
	 }
	 
}
