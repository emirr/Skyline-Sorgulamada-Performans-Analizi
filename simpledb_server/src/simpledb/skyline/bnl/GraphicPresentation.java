package simpledb.skyline.bnl;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Shape;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

import javax.swing.JPanel;


//
//import java.awt.BasicStroke;
//import java.awt.Color;
//import java.awt.Graphics;
//import java.awt.Graphics2D;
//import java.awt.RenderingHints;
//import java.awt.event.ActionEvent;
//import java.awt.event.ActionListener;
//import java.awt.geom.Point2D;
//import java.awt.image.BufferedImage;
//import java.util.HashMap;
//import java.util.Map;
//
//import javax.swing.JPanel;
//
  public class GraphicPresentation extends JPanel implements ActionListener {
//
	ArrayList<Shape> points = new ArrayList<>();
	Dimension frameSize = new Dimension();
//	private int[][] allPoints;
//	private int[][] skylinePoints;
//
//	private HashMap<Point2D, Boolean> pointList = new HashMap<>();
//
//	public GraphicPresentation(int[][] mtrx1, int[][] mtrx2) {
//
//		// defaultColor = Color.WHITE;// arka plan rengi bu
//		setBackground(Color.RED);
//		this.allPoints = mtrx1;
//		this.skylinePoints = mtrx2;
//		System.out.println("skylinepoint length:" +
//				 skylinePoints.length);
//				 System.out.println("allpoint length:" +
//						 allPoints.length);
//	}
 
//	void determineColorOfPoints() {
//		for (int i = 0; i < allPoints.length; i++) {
//
//			int k = 0;
//			int l = 0;
//		//	System.out.print("okunan index:" + i);
//		//	System.out.println("okunan nokta:" + allPoints[i][0] + "-" + allPoints[i][1]);
//			while (k < skylinePoints.length) {
//			//	System.out.println("k:" + k);
//				if ((allPoints[i][0] == skylinePoints[k][0]) && (allPoints[i][1] == skylinePoints[k][1])) {
//					setPoint(i, true);
//					l++;
//				}
//
//				k++;
//			}
//			if (l == 0)
//				setPoint(i, false);
//
//		}
//
//	}
//
//	void setPoint(int indexOfTuple, boolean skylineState) {
//
//		double x = (double) allPoints[indexOfTuple][0];
//		double y = (double) allPoints[indexOfTuple][1];
//		Point2D p = new Point2D.Double(x, y);
//		//System.out.print("poinlist size:" + pointList.size() + " ");
//		pointList.put(p, skylineState);
//		
////		System.out.print("index:" + indexOfTuple);
////		System.out.println("map edilen de�er:" + x + "-" + y);
//	}
//
	public GraphicPresentation (Dimension frameSize){
		this.frameSize = frameSize;
	}
    private void doDrawing(Graphics g) {
//
		Graphics2D g2 = (Graphics2D) g;
		//AffineTransform at = g2.getTransform();
		g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		//g2.tr
		Color color;
		//int i = 0;
		for (int i = 0; i < Correlated.list.length; i++) {
			int x =(int) Correlated.list[i][0];
			if(x < 0)
				x = -x;
			//System.out.println("x:" + x);
			int y =(int)  Correlated.list[i][1];
			if(y < 0)
				y = -y;
			//Shape s = new Line2D.Double(x, y, x, y);
			
			color = Color.BLACK;
			g2.setPaint(color);
			g2.setStroke(new BasicStroke(2));
			
		
			//i = i + 2;
			System.out.println("heigt:" + frameSize.getHeight());
			y =(int) frameSize.getHeight() - y;
			g2.drawLine(x, y, x, y);
		}
//		Point2D e;
//
//		Color color;
//		//
//		determineColorOfPoints();
//		System.out.println("pointlist size:" + pointList.size());
//		for (Map.Entry<Point2D, Boolean> entry : pointList.entrySet()) {
//			e = entry.getKey();
//			if (pointList.get(e))
//				color = Color.BLACK;
//			else
//				color = Color.YELLOW;
//			g2.setPaint(color);
//
//			int x1 = (int) e.getX();
//			int y1 = (int) e.getY();
//			
////			System.out.print("i�aretli  de�er:" + x1);
////			System.out.print("-" + y1);
////			if(color.equals(Color.BLACK))
////				System.out.println(" skyline de�eri ");
////			else
////				System.out.println();
//			g2.setStroke(new BasicStroke(4));
//			g2.drawLine(x1, y1, x1, y1);
//			// i++;
//			// System.out.println("... belirleme i�lemi tamam." + i);
//		}
	}
//
	@Override
	public void paintComponent(Graphics g) {

		super.paintComponent(g);
		doDrawing(g);
	}
//
	@Override
	public void actionPerformed(ActionEvent e) {
		repaint();
	}
	

} // end class GraphicPresentation
