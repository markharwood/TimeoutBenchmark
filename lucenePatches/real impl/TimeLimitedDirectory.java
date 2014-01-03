package org.apache.lucene.store;

import java.io.IOException;

import org.apache.lucene.search.ActivityTimeMonitor;
import org.apache.lucene.store.Directory.IndexInputSlicer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class TimeLimitedDirectory extends FilterDirectory {

    public TimeLimitedDirectory(Directory in) {
        super(in);
    }
    
    

    @Override
    public IndexInputSlicer createSlicer(String name, IOContext context) throws IOException {
        IndexInputSlicer result = super.createSlicer(name, context);
        return new TimeLimitedIndexInputSlicer(result);
    }



    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return new TimeLimitedIndexInput(name,super.openInput(name, context));
    }
    class TimeLimitedIndexInputSlicer extends IndexInputSlicer{

        private IndexInputSlicer in;

        public TimeLimitedIndexInputSlicer(IndexInputSlicer in) {
            this.in=in;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public IndexInput openSlice(String sliceDescription, long offset, long length) throws IOException {
            return new TimeLimitedIndexInput(sliceDescription, in.openSlice(sliceDescription, offset, length));
        }
        
    }
    
    static class TimeLimitedIndexInput extends IndexInput{

        IndexInput in;
        private String name;
        
        public TimeLimitedIndexInput(String resourceDescription, IndexInput in) {
            super(resourceDescription);
            this.in = in;
            this.name=resourceDescription;
        }

        @Override
        public void close() throws IOException {
            ActivityTimeMonitor.checkForTimeout();
            in.close();
        }

        @Override
        public long getFilePointer() {
            ActivityTimeMonitor.checkForTimeout();
            return in.getFilePointer();
        }

        @Override
        public void seek(long pos) throws IOException {
            ActivityTimeMonitor.checkForTimeout();
            in.seek(pos);                
        }

        @Override
        public long length() {
            ActivityTimeMonitor.checkForTimeout();
            return in.length();
        }

        @Override
        public byte readByte() throws IOException {
            ActivityTimeMonitor.checkForTimeout();
            return in.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            ActivityTimeMonitor.checkForTimeout();
            in.readBytes(b,offset,len);
        }
        
        
        @Override public IndexInput clone() { 
            //Only wrap in timer for search threads subject to timed activities.
            long timeoutInMs=ActivityTimeMonitor.getCurrentThreadTimeout();
            if(timeoutInMs>0){
                return new TimeLimitedIndexInput(name,in.clone()); 
            } 
            return in.clone(); 
        }
        @Override public boolean equals(Object o) { return in.equals(o); }
        @Override public int hashCode() { return in.hashCode(); }
    }
}
