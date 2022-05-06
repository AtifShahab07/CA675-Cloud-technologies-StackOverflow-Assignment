-- Command to execute first map-reduce job

hadoop jar hadoop-streaming-3.2.2.jar -files /home/atif_shahab2/Python_MapReduce_mapper1.py,/home/atif_shahab2/Python_MapReduce_reducer1.py -mapper 'python Python_MapReduce_mapper1.py' -reducer 'python Python_MapReduce_reducer1.py' -input hdfs:///PostDataOutput2/part-r-00000 -output hdfs:///data/output1

-- Command to execute second map-reduce job

hadoop jar hadoop-streaming-3.2.2.jar -files /home/atif_shahab2/Python_MapReduce_mapper2.py,/home/atif_shahab2/Python_MapReduce_reducer2.py -mapper 'python Python_MapReduce_mapper2.py' -reducer 'python Python_MapReduce_reducer2.py ' -input hdfs:///data/output1/ -output hdfs:///data/output2

-- Command to execute third map-reduce job

hadoop jar hadoop-streaming-3.2.2.jar -files /home/atif_shahab2/Python_MapReduce_mapper3.py,/home/atif_shahab2/Python_MapReduce_reducer3.py -mapper 'python Python_MapReduce_mapper3.py' -reducer 'python Python_MapReduce_reducer3.py ' -input hdfs:///data/output2 -output hdfs:///data/output3

-- Command to execute fourth map-reduce job

hadoop jar hadoop-streaming-3.2.2.jar -files /home/atif_shahab2/Python_MapReduce_mapper4.py -numReduceTasks 0 -input hdfs:///data/output3/ -output hdfs:///data/output4 -mapper 'python Python_MapReduce_mapper4.py'

