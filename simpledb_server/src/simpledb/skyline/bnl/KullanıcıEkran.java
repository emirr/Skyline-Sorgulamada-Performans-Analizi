package simpledb.skyline.bnl;

import java.awt.Color;
import java.awt.Component;
import java.awt.EventQueue;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Vector;

import javax.swing.BorderFactory;
//import javax.swing.BorderFactory;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SpringLayout;
import javax.swing.border.EmptyBorder;
import javax.swing.border.TitledBorder;

import simpledb.metadata.MetadataMgr;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class KullanýcýEkran extends JFrame {

	private JPanel contentPane;
	private JTextField textField;
	private JTextField textField_1;
	private JTextField textField_2;
	private JTextField textField_3;
	private JTextField textField_4;
	JButton btnAddField;
	private Schema sch = new Schema();
	Schema sch2 = new Schema();
	private String tmpStr, tmpStr2, tmpStr3, tmpStr4, tmpStr5, tmpStr6;
	private boolean fieldBuildCompleteStat = false;
	public Test1 test1;
	public Deneme1 deneObj;
	static int clickCount;
	MetadataMgr mg;
	
	Transaction tx ;
	
	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					KullanýcýEkran frame = new KullanýcýEkran();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 */
	public KullanýcýEkran() {
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(500, 500, 450, 500);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		SpringLayout sl_contentPane = new SpringLayout();
		contentPane.setLayout(sl_contentPane);

		JPanel mevcutVTPanel = new JPanel();
		sl_contentPane.putConstraint(SpringLayout.NORTH, mevcutVTPanel, 22, SpringLayout.NORTH, contentPane);
		sl_contentPane.putConstraint(SpringLayout.WEST, mevcutVTPanel, 0, SpringLayout.WEST, contentPane);
		sl_contentPane.putConstraint(SpringLayout.SOUTH, mevcutVTPanel, -356, SpringLayout.SOUTH, contentPane);
		sl_contentPane.putConstraint(SpringLayout.EAST, mevcutVTPanel, 0, SpringLayout.EAST, contentPane);
		JLabel lblVtSe = new JLabel("VT se\u00E7 :");
		sl_contentPane.putConstraint(SpringLayout.NORTH, lblVtSe, 35, SpringLayout.NORTH, contentPane);
		sl_contentPane.putConstraint(SpringLayout.WEST, lblVtSe, 20, SpringLayout.WEST, contentPane);
		mevcutVTPanel.add(lblVtSe);

		Vector comboBoxItems = new Vector();
		comboBoxItems.add("-");
		// System.out.println(" " get);
		for (String s : findDB())
			comboBoxItems.add(s);
		// comboBox_4.setModel(new DefaultComboBoxModel(new String[] {
		// "jhkjghkjgkjgkg" }));
		final DefaultComboBoxModel model = new DefaultComboBoxModel(comboBoxItems);
		JComboBox comboBox_4 = new JComboBox(model);
		// System.out.println(" " + getC);
		sl_contentPane.putConstraint(SpringLayout.NORTH, comboBox_4, -3, SpringLayout.NORTH, lblVtSe);
		sl_contentPane.putConstraint(SpringLayout.WEST, comboBox_4, 6, SpringLayout.EAST, lblVtSe);
		mevcutVTPanel.add(comboBox_4);

		JLabel lblTabloSe = new JLabel("Tablo Se\u00E7 :");
		sl_contentPane.putConstraint(SpringLayout.EAST, comboBox_4, -151, SpringLayout.WEST, lblTabloSe);
		sl_contentPane.putConstraint(SpringLayout.NORTH, lblTabloSe, 0, SpringLayout.NORTH, lblVtSe);
		sl_contentPane.putConstraint(SpringLayout.EAST, lblTabloSe, -128, SpringLayout.EAST, contentPane);
		mevcutVTPanel.add(lblTabloSe);

		// Vector comboItem = new Vector();
		// String a = (String) comboBox_4.getSelectedItem();
		// if(!comboBox_4.getSelectedItem().equals("-")){
		// for(String s : findTable(a))
		// comboItem.add(s);
		// }
		// final DefaultComboBoxModel model4 = new
		// DefaultComboBoxModel(comboItem);

		JTextArea textArea = new JTextArea();

		mevcutVTPanel.add(textArea);
		// JComboBox comboBox_5 = new JComboBox();
		// //comboBox_5.setModel(new DefaultComboBoxModel(new String[] {
		// "jh\u015Fjh\u015Fjh\u015Fljhhl\u015F" }));
		// sl_contentPane.putConstraint(SpringLayout.NORTH, comboBox_5, -3,
		// SpringLayout.NORTH, lblVtSe);
		// sl_contentPane.putConstraint(SpringLayout.WEST, comboBox_5, 6,
		// SpringLayout.EAST, lblTabloSe);
		// mevcutVTPanel.add(comboBox_5);
		javax.swing.border.Border lineBorder47 = BorderFactory.createLineBorder(Color.BLACK);

		mevcutVTPanel.setBorder(
				BorderFactory.createTitledBorder(lineBorder47, "Hazýr VT Seçimi", TitledBorder.LEFT, TitledBorder.TOP));
		contentPane.add(mevcutVTPanel);
		JPanel tabloBilgiPanel = new JPanel();
		sl_contentPane.putConstraint(SpringLayout.NORTH, tabloBilgiPanel, 6, SpringLayout.SOUTH, mevcutVTPanel);
		sl_contentPane.putConstraint(SpringLayout.WEST, tabloBilgiPanel, 10, SpringLayout.WEST, mevcutVTPanel);
		sl_contentPane.putConstraint(SpringLayout.EAST, tabloBilgiPanel, 0, SpringLayout.EAST, mevcutVTPanel);

		// JComboBox comboBox_4 = new JComboBox();
		// mevcutVTPanel.add(comboBox_4);

		// sl_contentPane.putConstraint(SpringLayout.WEST, mevcutVTPanel, 0,
		// SpringLayout.WEST, tabloBilgiPanel);
		// sl_contentPane.putConstraint(SpringLayout.SOUTH, mevcutVTPanel, -6,
		// SpringLayout.NORTH, tabloBilgiPanel);
		// sl_contentPane.putConstraint(SpringLayout.EAST, mevcutVTPanel, 0,
		// SpringLayout.EAST, tabloBilgiPanel);
		JButton btnTabloSe = new JButton("Tablo Se\u00E7");
		btnTabloSe.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				//System.out.println("tablo seçiliyor...");
				JPanel al = new JPanel();
				String s = (String) comboBox_4.getSelectedItem();
				//System.out.println("tablo seçim vt adý" + s);
				// findTable(s);
				for (String field : findTable(s)) {

					JRadioButton box = new JRadioButton(field);

					al.add(box);
				}
				int res = JOptionPane.showConfirmDialog(null, al);

				if (res == JOptionPane.OK_OPTION) {
					Component[] components = al.getComponents();
					Component component = null;
					for (int i = 0; i < components.length; i++) {
						component = components[i];
						if (component instanceof JRadioButton) {
							JRadioButton cb = (JRadioButton) component;
							if (cb.isSelected())
								tmpStr = cb.getText();// tablo adý
							
							textArea.setText(tmpStr);
							// skylineFields.add(cb.getText());//gerçeklemede
							// init kullanmak yerine bir ArrayList<Stirng>
							// kullan.
							//
						}
					}
				}

				// System.out.println("skyline alanlarý:");
				// for(String field : skylineFields){
				// System.out.println(" " + field);
				// }
//				System.out.println("ss"+s);
				//tmpStr6 = (String) comboBox_3.getSelectedItem();//blocksize
				SimpleDB.init(s);
				mg = SimpleDB.mdMgr();
				tx = new Transaction();
//				System.out.println("" +tx);
				int indexOf = tmpStr.indexOf(".");
				System.out.println(". index:" + indexOf);
				System.out.println("ilk hali:" + tmpStr);
				tmpStr = tmpStr.substring(0, indexOf);
				System.out.println("son hali:" + tmpStr);
				if(mg == null)
					System.out.println("mg null");
				TableInfo ti = mg.getTableInfo(tmpStr, tx);
				System.out.println("table name:" + ti.fileName() + " " + ti.recordLength() );
//				tx.commit();
				System.out.println("seçilen hazýr tablonun alanlarý:");
				for(String schm: ti.schema().fields())
					System.out.println(" " + schm);
				sch = ti.schema();
				deneObj = new Deneme1(sch); 
				tx.commit();
			}
		});

		mevcutVTPanel.add(btnTabloSe);
		// sl_contentPane.putConstraint(SpringLayout.NORTH, tabloBilgiPanel,
		// 110, SpringLayout.NORTH, contentPane);
		// sl_contentPane.putConstraint(SpringLayout.WEST, tabloBilgiPanel, 0,
		// SpringLayout.WEST, contentPane);

		SpringLayout sl_tabloBilgiPanel = new SpringLayout();
		// sl_tabloBilgiPanel.putConstraint(SpringLayout.WEST, lblWindowBuffer,
		// 168, SpringLayout.WEST, tabloBilgiPanel);

		JLabel lblVtAd = new JLabel("VT ad\u0131:");
		tabloBilgiPanel.add(lblVtAd);
		sl_contentPane.putConstraint(SpringLayout.NORTH, lblVtAd, 10, SpringLayout.NORTH, contentPane);
		sl_contentPane.putConstraint(SpringLayout.WEST, lblVtAd, 0, SpringLayout.WEST, contentPane);
		lblVtAd.setLabelFor(textField_4);

		textField_4 = new JTextField();
		tabloBilgiPanel.add(textField_4);
		sl_contentPane.putConstraint(SpringLayout.SOUTH, textField_4, -375, SpringLayout.SOUTH, contentPane);
		sl_contentPane.putConstraint(SpringLayout.WEST, textField_4, 194, SpringLayout.WEST, contentPane);
		textField_4.setColumns(10);
		JLabel lblNewLabel = new JLabel("Tablo adý: ");
		sl_tabloBilgiPanel.putConstraint(SpringLayout.NORTH, lblNewLabel, 15, SpringLayout.NORTH, tabloBilgiPanel);
		sl_tabloBilgiPanel.putConstraint(SpringLayout.WEST, lblNewLabel, 5, SpringLayout.EAST, tabloBilgiPanel);
		sl_tabloBilgiPanel.putConstraint(SpringLayout.EAST, lblNewLabel, 5, SpringLayout.WEST, tabloBilgiPanel);
		tabloBilgiPanel.add(lblNewLabel);
		// sl_tabloBilgiPanel.putConstraint(SpringLayout.NORTH, textField_1, -3,
		// SpringLayout.NORTH, lblNewLabel);
		// sl_tabloBilgiPanel.putConstraint(SpringLayout.WEST, textField_1, 6,
		// SpringLayout.EAST, lblNewLabel);
		// sl_alanBilgiPanel.putConstraint(SpringLayout.WEST, lblAlanTipi, 0,
		// SpringLayout.WEST, lblNewLabel);

		textField = new JTextField();
		sl_tabloBilgiPanel.putConstraint(SpringLayout.EAST, textField, -15, SpringLayout.EAST, contentPane);
		tabloBilgiPanel.add(textField);
		textField.setColumns(10);
		sl_tabloBilgiPanel.putConstraint(SpringLayout.NORTH, textField, -3, SpringLayout.NORTH, lblNewLabel);
		lblNewLabel.setLabelFor(textField);
		// sl_tabloBilgiPanel.putConstraint(SpringLayout.WEST, textField, 0,
		// SpringLayout.WEST, lblKaytSays);
		// sl_alanBilgiPanel.putConstraint(SpringLayout.WEST, comboBox, 0,
		// SpringLayout.WEST, textField);
		// sl_alanBilgiPanel.putConstraint(SpringLayout.EAST, comboBox, -56,
		// SpringLayout.EAST, textField);

		JLabel lblKaytSays = new JLabel("Kay\u0131t say\u0131s\u0131: ");
		sl_tabloBilgiPanel.putConstraint(SpringLayout.NORTH, lblKaytSays, 15, SpringLayout.NORTH, tabloBilgiPanel);
		tabloBilgiPanel.add(lblKaytSays);

		textField_1 = new JTextField();
		sl_tabloBilgiPanel.putConstraint(SpringLayout.WEST, lblKaytSays, 136, SpringLayout.EAST, textField_1);
		sl_tabloBilgiPanel.putConstraint(SpringLayout.EAST, lblKaytSays, -222, SpringLayout.WEST, textField_1);
		lblKaytSays.setLabelFor(textField_1);
		tabloBilgiPanel.add(textField_1);
		textField_1.setColumns(7);

		javax.swing.border.Border lineBorder3 = BorderFactory.createLineBorder(Color.BLACK);

		tabloBilgiPanel.setBorder(BorderFactory.createTitledBorder(lineBorder3, "Yeni Tablo Bilgisi", TitledBorder.LEFT,
				TitledBorder.TOP));

		contentPane.add(tabloBilgiPanel);

		JPanel alanBilgiPanel = new JPanel();
		sl_contentPane.putConstraint(SpringLayout.SOUTH, tabloBilgiPanel, -22, SpringLayout.NORTH, alanBilgiPanel);
		sl_contentPane.putConstraint(SpringLayout.NORTH, alanBilgiPanel, 215, SpringLayout.NORTH, contentPane);
		sl_contentPane.putConstraint(SpringLayout.WEST, alanBilgiPanel, 0, SpringLayout.WEST, contentPane);
		SpringLayout sl_alanBilgiPanel = new SpringLayout();
		sl_alanBilgiPanel.putConstraint(SpringLayout.EAST, alanBilgiPanel, -5, SpringLayout.EAST, alanBilgiPanel);

		JLabel lblAlanTipi = new JLabel("Alan tipi: ");
		alanBilgiPanel.add(lblAlanTipi);

		JComboBox comboBox = new JComboBox();
		sl_alanBilgiPanel.putConstraint(SpringLayout.NORTH, comboBox, -3, SpringLayout.NORTH, lblAlanTipi);
		comboBox.setModel(new DefaultComboBoxModel(new String[] { "int", "String" }));
		comboBox.setSelectedIndex(0);
		lblAlanTipi.setLabelFor(comboBox);
		alanBilgiPanel.add(comboBox);

		JLabel lblAlanAd = new JLabel("Alan ad\u0131:");
		sl_alanBilgiPanel.putConstraint(SpringLayout.NORTH, lblAlanAd, 0, SpringLayout.NORTH, lblAlanTipi);
		sl_alanBilgiPanel.putConstraint(SpringLayout.WEST, lblAlanAd, 32, SpringLayout.EAST, comboBox);
		alanBilgiPanel.add(lblAlanAd);

		textField_2 = new JTextField();
		sl_alanBilgiPanel.putConstraint(SpringLayout.NORTH, textField_2, -3, SpringLayout.NORTH, lblAlanTipi);
		lblAlanAd.setLabelFor(textField_2);
		alanBilgiPanel.add(textField_2);
		textField_2.setColumns(8);

		JLabel lblUzunluk = new JLabel("Uzunluk:");
		sl_alanBilgiPanel.putConstraint(SpringLayout.EAST, textField_2, -27, SpringLayout.WEST, lblUzunluk);
		sl_alanBilgiPanel.putConstraint(SpringLayout.NORTH, lblAlanTipi, 0, SpringLayout.NORTH, lblUzunluk);
		sl_alanBilgiPanel.putConstraint(SpringLayout.NORTH, lblUzunluk, 19, SpringLayout.SOUTH, textField_1);
		// sl_contentPane.putConstraint(SpringLayout.NORTH, lblNewLabel_1, 0,
		// SpringLayout.NORTH, lblWindowBuffer);

		JLabel lblNewLabel_1 = new JLabel("Veri da\u011F\u0131l\u0131m\u0131:\r\n");
		tabloBilgiPanel.add(lblNewLabel_1);
		JComboBox comboBox_2 = new JComboBox();
		lblNewLabel_1.setLabelFor(comboBox_2);
		// sl_contentPane.putConstraint(SpringLayout.WEST, lblNewLabel_1, 266,
		// SpringLayout.WEST, contentPane);
		// sl_contentPane.putConstraint(SpringLayout.EAST, lblNewLabel_1, -79,
		// SpringLayout.EAST, contentPane);
		// sl_contentPane.putConstraint(SpringLayout.WEST, comboBox_2, 6,
		// SpringLayout.EAST, lblNewLabel_1);

		
		tabloBilgiPanel.add(comboBox_2);
		sl_contentPane.putConstraint(SpringLayout.NORTH, comboBox_2, 302, SpringLayout.NORTH, contentPane);
		comboBox_2.setModel(new DefaultComboBoxModel(new String[] { "indp.", "corr", "anticorrr" }));
		comboBox_2.setSelectedIndex(0);
		
		alanBilgiPanel.add(lblUzunluk);

		textField_3 = new JTextField();
		lblUzunluk.setLabelFor(textField_3);
		alanBilgiPanel.add(textField_3);
		textField_3.setColumns(5);

		btnAddField = new JButton("Add Field");

		btnAddField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		sl_alanBilgiPanel.putConstraint(SpringLayout.NORTH, btnAddField, 13, SpringLayout.SOUTH, textField_2);
		// sl_alanBilgiPanel.putConstraint(SpringLayout.WEST, btnAddField, 0,
		// SpringLayout.WEST, lblAlanAd);
		alanBilgiPanel.add(btnAddField);

		javax.swing.border.Border lineBorder = BorderFactory.createLineBorder(Color.BLACK);

		alanBilgiPanel.setBorder(
				BorderFactory.createTitledBorder(lineBorder, "Yeni Alan Bilgisi", TitledBorder.LEFT, TitledBorder.TOP));

		contentPane.add(alanBilgiPanel);

		btnAddField.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				tmpStr = textField_2.getText();// alan adý alýndý

				tmpStr2 = textField_3.getText();// uzunluk bilgisi alýndý
				// if(tmpStr2 == null)
				// tmpStr2 = "0";
				tmpStr3 = (String) comboBox.getSelectedItem();// alan tipi
																// alýndý
				addAnyField(tmpStr, tmpStr3, tmpStr2);
				// Schema sch1 = addAnyField(tmpStr, tmpStr3, tmpStr2);
				System.out.println("yeni alan eklendi");

			}
		});
		JPanel ayarPanel = new JPanel();
		sl_contentPane.putConstraint(SpringLayout.SOUTH, alanBilgiPanel, -21, SpringLayout.NORTH, ayarPanel);
		sl_contentPane.putConstraint(SpringLayout.NORTH, ayarPanel, 306, SpringLayout.NORTH, contentPane);
		sl_contentPane.putConstraint(SpringLayout.SOUTH, ayarPanel, -53, SpringLayout.SOUTH, contentPane);
		sl_contentPane.putConstraint(SpringLayout.WEST, ayarPanel, 0, SpringLayout.WEST, contentPane);
		sl_contentPane.putConstraint(SpringLayout.EAST, ayarPanel, 0, SpringLayout.EAST, contentPane);

		JLabel lblAlgoritmaTipi = new JLabel("Algoritma tipi:");
		ayarPanel.add(lblAlgoritmaTipi);
		// sl_contentPane.putConstraint(SpringLayout.NORTH, lblAlgoritmaTipi, 0,
		// SpringLayout.NORTH, lblNewLabel_1);
		// sl_contentPane.putConstraint(SpringLayout.WEST, lblAlgoritmaTipi, 0,
		// SpringLayout.WEST, btnAlanlarTamam);
		// sl_contentPane.putConstraint(SpringLayout.WEST, comboBox_1, 6,
		// SpringLayout.EAST, lblAlgoritmaTipi);

		JComboBox comboBox_1 = new JComboBox();
		lblAlgoritmaTipi.setLabelFor(comboBox_1);
		sl_contentPane.putConstraint(SpringLayout.NORTH, comboBox_1, 302, SpringLayout.NORTH, contentPane);
		sl_contentPane.putConstraint(SpringLayout.EAST, comboBox_1, -282, SpringLayout.EAST, contentPane);
		comboBox_1.setModel(
				new DefaultComboBoxModel(new String[] { "-", "basic bnl", "rplc win. bnl", "self org. bnl" }));
		comboBox_1.setSelectedIndex(2);
		ayarPanel.add(comboBox_1);

		JLabel lblWindowBuffer = new JLabel("BufferSize:\r\n");
		sl_contentPane.putConstraint(SpringLayout.NORTH, lblWindowBuffer, 3, SpringLayout.NORTH, comboBox_1);
		// sl_contentPane.putConstraint(SpringLayout.WEST, lblWindowBuffer, 19,
		// SpringLayout.EAST, btnSkylinezellikSe_1);
		ayarPanel.add(lblWindowBuffer);
		// sl_contentPane.putConstraint(SpringLayout.WEST, lblWindowBuffer, -59,
		// SpringLayout.WEST, comboBox_3);
		// sl_contentPane.putConstraint(SpringLayout.EAST, lblWindowBuffer, -6,
		// SpringLayout.WEST, comboBox_3);
		// sl_tabloBilgiPanel.putConstraint(SpringLayout.WEST, comboBox_3, 6,
		// SpringLayout.EAST, lblWindowBuffer);

		JComboBox comboBox_3 = new JComboBox();
		lblWindowBuffer.setLabelFor(comboBox_3);
		ayarPanel.add(comboBox_3);
		sl_contentPane.putConstraint(SpringLayout.WEST, comboBox_3, 214, SpringLayout.WEST, contentPane);
		sl_contentPane.putConstraint(SpringLayout.NORTH, comboBox_3, 0, SpringLayout.NORTH, comboBox_1);

		comboBox_3.setModel(new DefaultComboBoxModel(new String[] { "4", "6", "7", "8", "50" }));
		comboBox_3.setSelectedIndex(0);

		javax.swing.border.Border lineBorder33 = BorderFactory.createLineBorder(Color.BLACK);

		ayarPanel.setBorder(BorderFactory.createTitledBorder(lineBorder33, "Hesaplama Tercihleri", TitledBorder.LEFT,
				TitledBorder.TOP));

		contentPane.add(ayarPanel);

		// JPanel graphicPanel = new JPanel();
		// sl_contentPane.putConstraint(SpringLayout.SOUTH, lblAlgoritmaTipi,
		// -55, SpringLayout.NORTH, graphicPanel);
		// // sl_contentPane.putConstraint(SpringLayout.SOUTH, comboBox_3, -26,
		// // SpringLayout.NORTH, graphicPanel);
		// sl_contentPane.putConstraint(SpringLayout.WEST, graphicPanel, 10,
		// SpringLayout.WEST, contentPane);
		// sl_contentPane.putConstraint(SpringLayout.EAST, graphicPanel, -3,
		// SpringLayout.EAST, contentPane);
		// sl_contentPane.putConstraint(SpringLayout.NORTH, graphicPanel, 234,
		// SpringLayout.NORTH, contentPane);
		// sl_contentPane.putConstraint(SpringLayout.SOUTH, graphicPanel, 0,
		// SpringLayout.SOUTH, contentPane);
		// javax.swing.border.Border lineBorder2 =
		// BorderFactory.createLineBorder(Color.BLACK);
		//
		// graphicPanel.setBorder(
		// BorderFactory.createTitledBorder(lineBorder2, "Grafik",
		// TitledBorder.LEFT, TitledBorder.TOP));
		//
		// contentPane.add(graphicPanel);

		JButton btnNewButton = new JButton("Skyline hesapla");
		sl_contentPane.putConstraint(SpringLayout.NORTH, btnNewButton, 15, SpringLayout.SOUTH, ayarPanel);
		sl_contentPane.putConstraint(SpringLayout.EAST, btnNewButton, -156, SpringLayout.EAST, contentPane);

		JButton btnAlanlarTamam = new JButton("Skyline Alan Se\u00E7");
		ayarPanel.add(btnAlanlarTamam);
		sl_contentPane.putConstraint(SpringLayout.NORTH, btnAlanlarTamam, 0, SpringLayout.NORTH, btnNewButton);
		sl_contentPane.putConstraint(SpringLayout.WEST, btnAlanlarTamam, 20, SpringLayout.WEST, contentPane);

		btnAlanlarTamam.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				// fieldBuildCompleteStat = true;
				System.out.println("alan ekleme tamamlandý");
				String a = (String) comboBox_4.getSelectedItem();
				if (a.equals("-")) {
					sch = sch2;
				}
				for (String field : sch.fields()) {
					System.out.println("skyline olabilir " + field);
				}
				deneObj = new Deneme1(sch);
			}
		});

		// sl_contentPane.putConstraint(SpringLayout.EAST, comboBox_2, 0,
		// SpringLayout.EAST, btnNewButton);
		// sl_contentPane.putConstraint(SpringLayout.SOUTH, btnNewButton, -6,
		// SpringLayout.NORTH, graphicPanel);
		// sl_contentPane.putConstraint(SpringLayout.EAST, btnNewButton, -10,
		// SpringLayout.EAST, contentPane);
		contentPane.add(btnNewButton);

		// JButton btnNewButton_1 = new JButton("Tablo Ekle");
		// btnNewButton_1.addActionListener(new ActionListener() {
		// public void actionPerformed(ActionEvent e) {
		// tmpStr = textField.getText();
		//
		// }
		// });
		// tabloBilgiPanel.add(btnNewButton_1);

		btnNewButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				String a = (String) comboBox_4.getSelectedItem();
				if (a.equals("-")) {
					tmpStr = textField.getText();//yeni tablo adý
					tmpStr2 = textField_1.getText();// kayýt sayýsý int'e çevrilmeli
					tmpStr3 = textField_4.getText();// vt adý
					tmpStr4 = (String) comboBox_1.getSelectedItem();// alg. tipi
					System.out.println("algo tip:" + tmpStr4);
					tmpStr5 = (String) comboBox_2.getSelectedItem();// dataset
																	// daðýlýmý
					tmpStr6 = (String) comboBox_3.getSelectedItem();//
					test1 = new Test1(deneObj.getSch(), deneObj.getSkylineFields());
					
					//test1 = new Test1(dene.getSch(), );
					if(test1 == null)
						System.out.println("test1 null");
					test1.execNewSystem(tmpStr4, Integer.parseInt(tmpStr6), tmpStr, Integer.parseInt(tmpStr2), tmpStr5,
							tmpStr3, clickCount);
					clickCount++;
					System.out.println("clickcount:" + clickCount);
				} else{
					//tmpStr3 = a;
					tmpStr = textArea.getText();//hazýr tablo adý
					int indexOf = tmpStr.indexOf(".");
					System.out.println(". index:" + indexOf);
					System.out.println("ilk hali:" + tmpStr);
					tmpStr = tmpStr.substring(0, indexOf);
					System.out.println("en son hali:" + tmpStr);
					test1 = new Test1(deneObj.getSch(), deneObj.getSkylineFields());
					tmpStr6 = (String) comboBox_3.getSelectedItem();//blocksize
					System.out.println("son blok size:" + Integer.parseInt(tmpStr6));
					tmpStr4 = (String) comboBox_1.getSelectedItem();// alg. tipi
					System.out.println("algo tip:" + tmpStr4);
					
					test1.execCurrentSystem(tmpStr,Integer.parseInt(tmpStr6),tmpStr4,clickCount);
					clickCount++;
					System.out.println("clickcount:" + clickCount);
				}

				// tmpStr = textField.getText();// tablo adý
				
				//test1 = new Test1(deneObj.getSch(), deneObj.getSkylineFields());
				

			}
		});

		JButton btnNewButton_1 = new JButton("Tablo Olu\u015Ftur");
		
				tabloBilgiPanel.add(btnNewButton_1);
				btnNewButton_1.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						String a = (String) comboBox_4.getSelectedItem();
						if (a.equals("-")) {// yeni vt
							for(String s : sch.fields())
								System.out.println("þema alaný1:" + s);										// kuruluyor
							System.out.println("tablo oluþturuluyor...");
							tmpStr = textField.getText();// tablo adý
							System.out.println(""+tmpStr);
							tmpStr2 = textField_1.getText();// kayýt sayýsý int'e
															// çevrilmeli
							System.out.println(""+tmpStr2);
							tmpStr3 = textField_4.getText();// vt adý
							System.out.println(""+tmpStr3);
							tmpStr5 = (String) comboBox_2.getSelectedItem();// dataset
							// daðýlýmý
							System.out.println(""+tmpStr5);
							tmpStr6 = (String) comboBox_3.getSelectedItem();//blocksize
//							test1 = new Test1(deneObj.getSch(), deneObj.getSkylineFields());
							System.out.println(""+tmpStr6);
							for(String s : sch.fields())
								System.out.println("þema alaný2:" + s);
							test1 = new Test1(sch);
							
							if(test1 == null)
								System.out.println("tablo oluþturma sýrasýnda test1 null");
							test1.createSystem(Integer.parseInt(tmpStr2), tmpStr5, tmpStr, tmpStr3, Integer.parseInt(tmpStr6),clickCount);
							sch2 = sch;
							sch = new Schema();
							if(!sch.hasField("a"))
								System.out.println("yeni þema atandý...");
							
						}// else {
//							tmpStr3 = a;//hazýr vt adý
//							tmpStr = textArea.getText();//hazýr tablo adý
//							
//						}
						// tmpStr = textField.getText();// tablo adý

//		tmpStr4 = (String) comboBox_1.getSelectedItem();// alg. tipi
//		System.out.println("algo tip:" + tmpStr4);
//
//		//test1 = new Test1(deneObj.getSch(), deneObj.getSkylineFields());
//		test1.execAllSystem(tmpStr4, Integer.parseInt(tmpStr6), tmpStr, Integer.parseInt(tmpStr2), tmpStr5,
//				tmpStr3, clickCount);
//		clickCount++;
//		System.out.println("clickcount:" + clickCount);
					}
				});
	}

	public boolean isFieldBuildComplete() {
		return fieldBuildCompleteStat;

	}

	public void addAnyField(String ad, String tip, String uzunluk) {
		if (tip.equals("int"))
			sch.addIntField(ad);
		else
			sch.addStringField(ad, Integer.parseInt(uzunluk));
		// return sch;
	}

	public String[] findDB() {
		String dirName = "C:\\Users\\oblmv2";
		File dir = new File(dirName);
		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith("vt");
			}
		};
		String[] children = dir.list(filter);
		if (children == null) {
			System.out.println("Either dir does not exist or is not a directory");
		} else {
			for (int i = 0; i < children.length; i++) {
				String filename = children[i];
				System.out.println(filename);
			}
		}
		return children;
	}

	public String[] findTable(String dbName) {

		String dirName = "C:\\Users\\oblmv2" + "\\" + dbName;
		System.out.println(dirName);
		File dir = new File(dirName);
		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File dir, String name) {

				return !(name.startsWith("fldcat") || name.startsWith("simpledb") || name.startsWith("tblcat")
						|| name.startsWith("temp") || name.startsWith("viewcat") || name.startsWith("idx"));

			}
		};
		String[] children = dir.list(filter);
		if (children == null) {
			System.out.println("Either dir does not exist or is not a directory");
		} else {
			for (int i = 0; i < children.length; i++) {
				String filename = children[i];
				System.out.println(filename);
			}
		}
		return children;
	}
}
