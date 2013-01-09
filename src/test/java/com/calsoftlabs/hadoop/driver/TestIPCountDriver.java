package com.calsoftlabs.hadoop.driver;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.calsoftlabs.hadoop.mapper.IPCountMapper;
import com.calsoftlabs.hadoop.reducer.IPCountReducer;

public class TestIPCountDriver {

	MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

	@Before
	public void setUp() {
		IPCountMapper mapper = new IPCountMapper();
		IPCountReducer reducer = new IPCountReducer();
		mapDriver = new MapDriver<Object, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.setReducer(reducer);
		mapReduceDriver = new MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		String input =  "<189>date=2013-01-06 time=10:59:48 devname=Fortigate-100A device_id=FG100A2105401797 log_id=0021000002 type=traffic subtype=allowed pri=notice status=accept vd=\"root\" dir_disp=org tran_disp=noop src=192.168.18.50";
		String str1 = " srcname=192.168.18.50 src_port=59980 dst=192.168.100.1 dstname=192.168.100.1 dst_port=53 tran_ip=N/A tran_port=0 service=53/udp proto=17 app_type=N/A duration=180 rule=2 policyid=2 identidx=0 sent=61 rcvd=189 shaper_drop_sent=0 shaper_drop_rcvd=0 perip_drop=0 shaper_sent_name=\"N/A\" shaper_rcvd_name=\"N/A\" perip_name=\"N/A\" sent_pkt=1 rcvd_pkt=1 vpn=\"VPN to BG Road\" src_int=\"dmz1\" dst_int=\"wan1\" SN=527802315 app=\"N/A\" app_cat=\"N/A\" user=\"N/A\" group=\"N/A\" carrier_ep=\"N/A\"";
		mapDriver.withInput(new Object(), new Text(input));
		mapDriver.withOutput(new Text("192.168.18.50"), new IntWritable(1));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("192.168.18.50"), values);
		reduceDriver.withOutput(new Text("192.168.18.50"), new IntWritable(2));
		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		String input =  "<189>date=2013-01-06 time=10:59:48 devname=Fortigate-100A device_id=FG100A2105401797 log_id=0021000002 type=traffic subtype=allowed pri=notice status=accept vd=\"root\" dir_disp=org tran_disp=noop src=192.168.18.50";
		mapReduceDriver.withInput(new Object(), new Text(input));
		mapReduceDriver.addOutput(new Text("192.168.18.50"), new IntWritable(1));
		mapReduceDriver.runTest();
	}
}
