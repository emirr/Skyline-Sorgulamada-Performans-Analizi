package simpledb.skyline.bnl;

import java.awt.Component;
import java.util.ArrayList;

import javax.swing.JCheckBox;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

import simpledb.record.Schema;

public class Deneme1 {
	ArrayList<String> skylineFields;
	
	Schema sch;
	
	public Deneme1(Schema sch){
		this.sch = sch;
		//System.out.println("sdad");

		skylineFields = new ArrayList<>();
		for (String field : sch.fields()){
			System.out.println("" + field);
		}
		dene();
		
	}
	public  void dene() {
		//InitOneTable init = new InitOneTable();
		
//		for(int i = 0; i<26; i++){
//			//System.out.println(" " + i);
//		 sch.addIntField(String.valueOf((char)( i + 65)));
//		 //System.out.println("field name:" + String.valueOf((char)(i + 65)) );
//		
//		}
		
		JPanel al = new JPanel();
		for (String field : sch.fields()){
		    JCheckBox box = new JCheckBox(field);
		    
		    al.add(box);
		}
		int res = JOptionPane.showConfirmDialog(null, al);
		
		if(res == JOptionPane.OK_OPTION){
		Component[] components = al.getComponents();
		Component component = null;
		for (int i = 0; i < components.length; i++)
		{
		component = components[i];
		if (component instanceof JCheckBox) {
			JCheckBox cb = (JCheckBox) component;
			if(cb.isSelected())
				skylineFields.add(cb.getText());//gerçeklemede init kullanmak yerine bir ArrayList<Stirng> kullan.
	    }
        }
		}
		
//		System.out.println("skyline alanlarý:");
//		for(String field : skylineFields){
//			System.out.println(" " + field);
//		}
//        checkBox.addChangeListener(new ChangeListener() {
//            @Override
//            public void stateChanged(ChangeEvent e){
//                if (e.getSource() == checkBox) {  
//                    if (checkBox.isSelected()) {
//                      JOptionPane.showMessageDialog(null,  "Message", "Alert",
//                            JOptionPane.INFORMATION_MESSAGE);
//                    }
//                }
//            }
//        });
		
    }
	public ArrayList<String> getSkylineFields() {
		return skylineFields;
	}
	public Schema getSch() {
		return sch;
	}
}

