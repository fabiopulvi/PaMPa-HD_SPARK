package pampa; /**
 * Created by fabio on 01/11/16.
 */

import java.io.Serializable;
import java.util.ArrayList;

import pampa.row;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;


public class table implements Serializable {
    private ArrayList<row> rows = new ArrayList<row>();
    private  ArrayList<Integer> projection = new ArrayList<Integer>();
    private int deleted = 0;
    private  int max=0;
    private  int min=1;

    public table () {};

    public table (int projection_first_step, ArrayList<row> rows) {
        //this.projection.clear();
        //this.projection.clear();
        this.projection.add(projection_first_step);
        //for (int r: this.projection) System.out.println("added:"+ r);
        this.rows=rows;
        for (row r: this.rows) if (r.getTid().size()>0) if (r.getTid().get(r.getTid().size()-1)>=max) max=r.getTid().get(r.getTid().size()-1);
        this.deleted=0;
    }

    public table (ArrayList<Integer> projection, ArrayList<row> rows) {
        //this.projection.clear();
        //this.projection.clear();
        this.projection.addAll(projection);
        //for (int r: this.projection) System.out.println("added:"+ r);
        this.rows=rows;
        for (row r: this.rows) if (r.getTid().size()>0) if (r.getTid().get(r.getTid().size()-1)>=max) max=r.getTid().get(r.getTid().size()-1);
        this.deleted=0;
    }


    public   ArrayList<row> getRows () {
        return rows;
    }
    public  ArrayList<Integer> getProjection () {
        return this.projection;
    }

    public  int getDeleted () {
        return deleted;
    }

    public  int getMax () {
        return max;
    }

    public  boolean isEmpty() {
        for (row r: rows) {
            if (r.getTid().size()>0)
                return FALSE;}
            return TRUE;
    }

}
