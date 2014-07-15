package com.polymr.osm.poi.inputs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**

A hadoop input format specifically for reading Open Street Map xml dumps (.osm dumps).
<p>
Yields one 'node', The hadoop key here will be the position
in the underlying input stream and the values will be the raw xml of a record as text.
*/

public class PoiParserrInput extends TextInputFormat {
	
	public static final String START_TAG_KEY = "xmlinput.start";
    public static final String END_TAG_KEY = "xmlinput.end";


    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        return new XmlRecordReader();
    }
   
    /**
     * XMLRecordReader class to read through a given xml document to output
     * xml blocks as records as specified by the start tag and end tag
     */
    public static class XmlRecordReader extends
            RecordReader<LongWritable, Text> {

    	  private byte[] startTag;
          private byte[] endTag;
          private long start;
          private long end;
          private FSDataInputStream fsin;
          private DataOutputBuffer buffer = new DataOutputBuffer();

          private LongWritable key = new LongWritable();
          private Text value = new Text();
          
  		@Override
  		public void initialize(InputSplit split, TaskAttemptContext context)
  				throws IOException, InterruptedException {
			  		//get the configuration
			  			Configuration conf = context.getConfiguration();
			  			
			  	    //convert the start tag into bytes 		
			            startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
			            
			         // convert the end tag into bytes    
			            endTag = conf.get(END_TAG_KEY).getBytes("utf-8");
			            
			          //create a file split to hold the split   
			            FileSplit fileSplit = (FileSplit) split;
			            
			          // open the input file and seek to the start of the split
		                start = fileSplit.getStart();  
		               
		              // seek for the end of the split from input file and set the offset  
		                end = start + fileSplit.getLength();
		                
		              // get the path to which the split is to be written   
		                Path file = fileSplit.getPath();

		              // get the file system object   
		                FileSystem fs = file.getFileSystem(conf);
		                
		              // open the data input stream   
		                fsin = fs.open(fileSplit.getPath());
		                
		              //write to the data input stream from the start element of the input 
		              //search for start element in the data input stream 
		                fsin.seek(start);
  		}
  		
		@Override
		public void close() throws IOException {
			fsin.close();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return  (fsin.getPos() - start) / (float) (end - start);
		}



		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (fsin.getPos() < end) {
                if (readUntilMatch(startTag, false)) {
                    try {
                        buffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
                            key.set(fsin.getPos());
                            value.set(buffer.getData(), 0,
                                    buffer.getLength());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
		}
		
		 private boolean readUntilMatch(byte[] match, boolean withinBlock)
                 throws IOException {
             int i = 0;
             while (true) {
                 int b = fsin.read();
                 // end of file:
                 if (b == -1)
                     return false;
                 // save to buffer:
                 if (withinBlock)
                     buffer.write(b);
                 // check if we're matching:
                 if (b == match[i]) {
                     i++;
                     if (i >= match.length)
                         return true;
                 } else
                     i = 0;
                 // see if we've passed the stop point:
                 if (!withinBlock && i == 0 && fsin.getPos() >= end)
                     return false;
             }
         }
     }
  }
