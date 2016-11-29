package sol.req.ml;

import org.apache.hadoop.util.ToolRunner;

/**
 * Created by tarun.walia on 11/29/2016.
 */
public class MRJobMain {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MRJob(), args);
        System.exit(exitCode);
    }
}
