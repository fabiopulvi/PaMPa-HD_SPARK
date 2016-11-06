package pampa; /**
 * Created by fabio on 01/11/16.
 */


import java.io.Serializable;
import java.util.ArrayList;

public class row implements Serializable {
    private String item;
    private ArrayList<Integer> transactions;

    public row (String item, ArrayList<Integer> transactions) {
        this.item=item;
        this.transactions=transactions;
    }

    public String getItem () {return this.item;}
    public ArrayList<Integer> getTid() {return this.transactions;};
}



