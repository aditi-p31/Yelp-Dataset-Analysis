import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.GridLayout;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.Border;

public class Test {
  public static void main(String args[]) {
      try {
          String yelping_since = "2012-12-01";
          SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd" );  // United States style of format.
          java.util.Date myDate = format.parse(yelping_since);
          System.out.println(new java.sql.Date(myDate.getTime()));
      } 
      catch (ParseException ex) {
          Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
      }
  }
}