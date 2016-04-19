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
import java.awt.geom.Line2D;
import java.util.ArrayList;
import java.util.HashSet;

import javax.swing.JPanel;

public class GraphicPresentation extends JPanel implements ActionListener {
	Object[][] skylinePointMatrix;
	Object[][] allTuples;
	Dimension frameSize;
	HashSet<Object> redPoints = new HashSet<>();

	public GraphicPresentation(Object[][] allTuples, ArrayList<Object> skylinePoints, Dimension frameSize) {
		this.allTuples = allTuples;
		// this.skylinePoints = skylinePoints;
		this.frameSize = frameSize;

		skylinePointMatrix = new Object[skylinePoints.size() / 2][2];
		for (int i = 0; i < (skylinePoints.size() / 2); i++) {

			skylinePointMatrix[i][0] = skylinePoints.get(2 * i);
			skylinePointMatrix[i][1] = skylinePoints.get(2 * i + 1);

		}
	}

	void determineColorOfPoints() {
		for (int i = 0; i < allTuples.length; i++) {

			int k = 0;
			// int l = 0;
			// System.out.print("okunan index:" + i);
			// System.out.println("okunan nokta:" + allTuples[i][0] + "-" +
			// allTuples[i][1]);
			while (k < skylinePointMatrix.length) {
				// System.out.println("k:" + k);
				if ((allTuples[i][0] == skylinePointMatrix[k][0]) && (allTuples[i][1] == skylinePointMatrix[k][1])) {
					redPoints.add(i);
					// l++;
					break;
				}

				k++;
			}
			// if (l == 0)
			// setPoint(i, false);

		}

	}

	private void doDrawing(Graphics g) {
		Graphics2D g2 = (Graphics2D) g;
		// AffineTransform at = g2.getTransform();
		g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		// g2.tr
		double x = 0;
		double y = 0;
		int x1 = 0;
		int y1 = 0;
		Color color = null;
		determineColorOfPoints();
		for (int i = 0; i < allTuples.length; i++) {
			if(allTuples[i][0] instanceof Double)
				x = (double) allTuples[i][0];
			else{
				x1 = (int) allTuples[i][0];
				x = (double) x1;
			}
			if(allTuples[i][1] instanceof Double)
				y = (double) allTuples[i][1];
			else{
				y1 = (int) allTuples[i][1];
				y = (double) y1;
			}

			int k = 0;
			int l = 0;
			// System.out.print("okunan index:" + i);
			// System.out.println("okunan nokta:" + allTuples[i][0] + "-" +
			// allTuples[i][1]);
			while (k < skylinePointMatrix.length) {
				// System.out.println("k:" + k);
				if ((allTuples[i][0].equals(skylinePointMatrix[k][0]))
						&& (allTuples[i][1].equals(skylinePointMatrix[k][1]))) {

					l++;
					//System.out.println("l sýfýr deðil.");
					break;
				}

				k++;
			}

			if (l == 0)
				color = Color.BLACK;
			else
				color = Color.RED;

			g2.setPaint(color);
			g2.setStroke(new BasicStroke(3));

			// i = i + 2;
			// System.out.println("heigt:" + frameSize.getHeight());
			y = (double) frameSize.getHeight() - y;
			Shape shp = new Line2D.Double(x, y, x, y);
//			int w = frameSize.width;// real width of canvas
//			int h = frameSize.height; // real height of canvas
//					// Translate used to make sure scale is centered
//			g2.translate(w/2, h/2);
//			g2.scale(1.1, 1.1);
//			g2.translate(-w/2, -h/2);
			// g2.drawLine(x, y, x, y);
			g2.draw(shp);
		}

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
