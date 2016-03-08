//package simpledb.skyline.bnl;
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
//public class GraphicPresentation extends JPanel implements ActionListener {
//
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
//
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
////		System.out.println("map edilen deðer:" + x + "-" + y);
//	}
//
//	private void doDrawing(Graphics g) {
//
//		Graphics2D g2 = (Graphics2D) g;
//		g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
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
////			System.out.print("iþaretli  deðer:" + x1);
////			System.out.print("-" + y1);
////			if(color.equals(Color.BLACK))
////				System.out.println(" skyline deðeri ");
////			else
////				System.out.println();
//			g2.setStroke(new BasicStroke(4));
//			g2.drawLine(x1, y1, x1, y1);
//			// i++;
//			// System.out.println("... belirleme iþlemi tamam." + i);
//		}
//	}
//
//	@Override
//	public void paintComponent(Graphics g) {
//
//		super.paintComponent(g);
//		doDrawing(g);
//	}
//
//	@Override
//	public void actionPerformed(ActionEvent e) {
//		repaint();
//	}
//} // end class GraphicPresentation
