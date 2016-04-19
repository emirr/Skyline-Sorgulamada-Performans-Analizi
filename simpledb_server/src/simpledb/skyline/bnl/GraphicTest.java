package simpledb.skyline.bnl;

import java.util.ArrayList;

import javax.swing.JFrame;

public class GraphicTest extends JFrame{


	 
	 public GraphicTest(Object[][] allTuples, ArrayList<Object> skylinePoints){
		pack();
		setLocationByPlatform(true);
		 setSize(300,300);
		// pack();
		 setLocationRelativeTo(null);
		final GraphicPresentation display = new GraphicPresentation(allTuples, skylinePoints, getSize());
		 add(display);
		 
		 setTitle("Points");
		 setVisible(true);
	   //  pack();
//	     setSize(300,300);
	     // setLocation(450, 50);
	      //  setBackground(Color.RED);
	        //setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		 setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
	      //  setBorder(BorderFactory.createLineBorder(Color.GRAY,3));
	      //  int cellSize = 600/GRID_SIZE; // Aim for about a 600-by-600 pixel board.
	        
	       
	        
	 }
	 
}
