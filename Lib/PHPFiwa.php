<?php

// This timeseries engine implements:
// Fixed Interval No Averaging

class PHPFiwa
{
    private $dir = "/var/lib/phpfiwa/";
    private $log;
    
    private $buffers = array();
    private $metadata_cache = array();
    
    private $filehandle = array();
    private $dpposition = array();
    
    /**
     * Constructor.
     *
     * @api
    */

    public function __construct($settings)
    {
        if (isset($settings['datadir'])) $this->dir = $settings['datadir'];
        
        $this->log = new EmonLogger(__FILE__);
    }
    
	public function create($id,$options)
    {
        $interval = (int) $options['interval'];
        if ($interval<5) $interval = 5;
        // Check to ensure we dont overwrite an existing feed
        if (!$meta = $this->get_meta($id))
        {
            $this->log->info("PHPFIWA:create creating feed id=$id");
            // Set initial feed meta data
            $meta = new stdClass();
            $meta->id = $id;
            $meta->start_time = 0;
            
            // Limitation's on feed interval's so that the next layer can always be produced from an 
            // integer number of datapoints from the layer below
            
            // layer intervals are also designed for most useful data export, minute, hourly, daily mean
            
            $meta->nlayers = 0;
            
            if ($interval==5 || $interval==10 || $interval==15 || $interval==20 || $interval==30) {
                $meta->nlayers = 4;
                $meta->npoints = array(0,0,0,0);
                $meta->interval = array($interval,60,600,3600);
            }
            
            if ($interval==60 || $interval==120 || $interval==300) {
                $meta->nlayers = 3;
                $meta->npoints = array(0,0,0);
                $meta->interval = array($interval,600,3600);
            }
            
            if ($interval==600 || $interval==1200 || $interval==1800) {
                $meta->nlayers = 2;
                $meta->npoints = array(0,0);
                $meta->interval = array($interval,3600);
            }
            
            if ($interval==3600) {
                $meta->nlayers = 1;
                $meta->npoints = array(0);
                $meta->interval = array($interval);
            }
            
            // If interval is outside of the allowed layer intervals
            if ($meta->nlayers==0) return false;
            // Save meta data
            $this->create_meta($id,$meta);
            
            $fh = fopen($this->dir.$meta->id."_0.dat", 'c+');
            fclose($fh);
            $fh = fopen($this->dir.$meta->id."_1.dat", 'c+');
            fclose($fh);
            $fh = fopen($this->dir.$meta->id."_2.dat", 'c+');
            fclose($fh);
            $fh = fopen($this->dir.$meta->id."_3.dat", 'c+');
            fclose($fh);
        }
        $feedname = "$id.meta";
        if (file_exists($this->dir.$feedname)) return true;
        return false;
    }
	
	public function create_meta($id,$meta)
    {		
		$id = (int) $id;
        $feedname = "$id.meta";
    
		$metafile = fopen($this->dir.$feedname, 'wb');
        
        if (!$metafile) {
            $this->log->warn("PHPFIWA:create_meta could not open meta data file id=".$meta->id);
            return false;
        }
        
        if (!flock($metafile, LOCK_EX)) {
            $this->log->warn("PHPFiwa:create_meta meta file id=".$meta->id." is locked by another process");
            fclose($metafile);
            return false;
        }
        	
        fwrite($metafile,pack("I",$meta->id));
        fwrite($metafile,pack("I",$meta->start_time)); 
        fwrite($metafile,pack("I",$meta->nlayers));
        foreach ($meta->npoints as $n) fwrite($metafile,pack("I",0));       // Legacy
        foreach ($meta->interval as $d) {
			fwrite($metafile,pack("I",$d));
			$this->log->info("Write interval = ".$d);
		}
    }
	
    public function get_meta($filename)
    {
		// Load metadata from cache if it exists
        if (isset($this->metadata_cache[$filename])) 
        {
			return $this->metadata_cache[$filename];
        }
        elseif (file_exists($this->dir.$filename.".meta"))
        {
            $meta = new stdClass();
            $meta->id = $filename;
        
            $metafile = fopen($this->dir.$filename.".meta", 'rb');

            $tmp = unpack("I",fread($metafile,4));
            $tmp = unpack("I",fread($metafile,4)); 
            $meta->start_time = $tmp[1];
            $tmp = unpack("I",fread($metafile,4)); 
            $meta->nlayers = $tmp[1];
            
			$meta->npoints = array();
            for ($i=0; $i<$meta->nlayers; $i++) {
              $tmp = unpack("I",fread($metafile,4));
			  $meta->npoints[$i] = $tmp[1];
            }
            
            $meta->interval = array();
            for ($i=0; $i<$meta->nlayers; $i++)
            {
              $tmp = unpack("I",fread($metafile,4)); 
              $meta->interval[$i] = $tmp[1];
            }
            fclose($metafile);
            
            $this->metadata_cache[$filename] = $meta;
            
            return $meta;
        }
        else
        {
            return false;
        }
    }
    
    public function readnext($filename)
    {
        if (!isset($this->filehandle[$filename])) {
            $this->filehandle[$filename] = fopen($this->dir.$filename."_0.dat", 'rb');
            $this->dpposition[$filename] = 0;
        }
        $fh = $this->filehandle[$filename];
        if (feof($fh)) return false;
        
        $meta = $this->get_meta($filename);

        $d = fread($fh,4);
        if (strlen($d)!=4) return false;
        
        $val = unpack("f",$d);
        $value = $val[1];
        
        $time = $meta->start_time + $this->dpposition[$filename] * $meta->interval[0];
        $this->dpposition[$filename] += 1;
        
        return array('time'=>$time, 'value'=>$value);
    }
	
    public function post($id,$timestamp,$value)
    {   
        //$this->log->info("PHPFiwa:post id=$id timestamp=$timestamp value=$value");
        $id = (int) $id;
        $timestamp = (int) $timestamp;
        $value = (float) $value;
        $now = time();
        $start = $now-(3600*24*365*5); // 5 years in past
        $end = $now+(3600*48);         // 48 hours in future
        
        if ($timestamp<$start || $timestamp>$end) {
            $this->log->warn("PHPFiwa:post timestamp out of range");
            return false;
        }
        
        $layer = 0;
        // If meta data file does not exist then exit
        if (!$meta = $this->get_meta($id)) {
            $this->log->warn("PHPFiwa:post failed to fetch meta id=$id");
            return false;
		}	
	        
        // Note: The following comment and line of code were designed to quantize random points. Removed because we are quantizing to the source data already.
        // Calculate interval that this datapoint belongs too
        //$timestamp = floor($timestamp / $meta->interval[$layer]) * $meta->interval[$layer];
        // If this is a new feed (npoints == 0) then set the start time to the current datapoint
        if ($meta->npoints[0] == 0 && $meta->start_time==0) {
            $meta->start_time = $timestamp;
            $this->create_meta($id,$meta);			
        }
        if ($timestamp < $meta->start_time) {
            $this->log->warn("PHPFiwa:post timestamp older than feed start time id=$id");
            return false; // in the past
        }
        // Calculate position in base data file of datapoint
        $point = floor(($timestamp - $meta->start_time) / $meta->interval[$layer]);
        $last_point = $meta->npoints[0] - 1;
        if ($point<=$last_point) {
             // $this->log->warn("PHPFiwa:post updating of datapoints to be made via update function id=$id");
             return false; // updating of datapoints to be made available via update function
        }
        
        $result = $this->update_layer($meta,$layer,$point,$timestamp,$value);
    }
    
    private function update_layer($meta,$layer,$point,$timestamp,$value)
    {
        foreach ($meta->interval as $d) {
            $this->log->info("Meta interval = ".$d);
        }

        
        $fh = fopen($this->dir.$meta->id."_$layer.dat", 'c+');
        if (!$fh) {
            $this->log->warn("PHPFiwa:update_layer could not open data file layer $layer id=".$meta->id);
            return false;
        }
        
        if (!flock($fh, LOCK_EX)) {
            $this->log->warn("PHPFiwa:update_layer data file for layer=$layer feedid=".$meta->id." is locked by another process");
            fclose($fh);
            return false;
        }
        
        // 1) Write padding
        $last_point = $meta->npoints[$layer] - 1;
        $padding = ($point - $last_point)-1;
        
        if ($padding>0) {
            if ($this->write_padding($fh,$meta->npoints[$layer],$padding)===false)
            {
                // Npadding returned false = max block size was exeeded
                $this->log->warn("PHPFiwa:update_layer padding max block size exeeded $padding id=".$meta->id);
                return false;
            }
        }
        
        // 2) Write new datapoint
        fseek($fh,4*$point);
        if (!is_nan($value)) fwrite($fh,pack("f",$value)); else fwrite($fh,pack("f",NAN));
        
        if ($point >= $meta->npoints[$layer])
        {
          $meta->npoints[$layer] = $point + 1;
        }
        // fclose($fh);
        
        // 3) Averaging
        $layer ++;
        if ($layer<$meta->nlayers)
        {        
            $start_time_avl = floor($meta->start_time / $meta->interval[$layer]) * $meta->interval[$layer];
            $timestamp_avl = floor($timestamp / $meta->interval[$layer]) * $meta->interval[$layer];
            $point_avl = ($timestamp_avl-$start_time_avl) / $meta->interval[$layer];
            $point_in_avl = ($timestamp - $timestamp_avl) / $meta->interval[$layer-1];
           
            $first_point = $point - $point_in_avl;
            
            if ($first_point<0) $first_point = 0;
            
            // Read in points
            fseek($fh, 4*$first_point);
            $d = fread($fh, 4 * ($point_in_avl+1));
            $count = strlen($d)/4;
            $d = unpack("f*",$d);
            fclose($fh);
        
            // Calculate average of points
            $sum_count = 0;
            $sum = 0.0;
            $i=0;
            while ($count--) {
                $i++;
                if (is_nan($d[$i])) continue;   // Skip unknown values
                $sum += $d[$i];                 // Summing
                $sum_count ++;
            }
            if ($sum_count>0) {
                $average = $sum / $sum_count;
            } else {
                $average = NAN;
            }
            			
			$meta = $this->update_layer($meta,$layer,$point_avl,$timestamp_avl,$average);

        }
        
        return $meta;
    }	
	
	/**
     * Get the last value from a feed
     *
     * @param integer $feedid The id of the feed
    */
    public function lastvalue($id)
    {
        $id = (int) $id;
        
        // If meta data file does not exist then exit
        if (!$meta = $this->get_meta($id)) return false;
        if ($meta->npoints[0]>0)
        {
            $fh = fopen($this->dir.$meta->id."_0.dat", 'rb');
            $size = $meta->npoints[0]*4;
            fseek($fh,$size-4);
            $d = fread($fh,4);
            fclose($fh);
            $val = unpack("f",$d);
            $time = $meta->start_time + $meta->interval[0] * $meta->npoints[0];
            
            return array('time'=>$time, 'value'=>$val[1]);
        }
        else
        {
            return array('time'=>0, 'value'=>0);
        }
    }

	private function write_padding($fh,$npoints,$npadding)
    {
        $tsdb_max_padding_block = 1024 * 1024;
        
        // Padding amount too large
        if ($npadding>$tsdb_max_padding_block*2) {
            return false;
        }
        // Maximum points per block
        $pointsperblock = $tsdb_max_padding_block / 4; // 262144
        // If needed is less than max set to padding needed:
        if ($npadding < $pointsperblock) $pointsperblock = $npadding;
        // Fill padding buffer
        $buf = '';
        for ($n = 0; $n < $pointsperblock; $n++) {
            $buf .= pack("f",NAN);
        }
        fseek($fh,4*$npoints);
        do {
            if ($npadding < $pointsperblock) 
            { 
                $pointsperblock = $npadding;
                $buf = ''; 
                for ($n = 0; $n < $pointsperblock; $n++) {
                    $buf .= pack("f",NAN);
                }
            }
            
            fwrite($fh, $buf);
            $npadding -= $pointsperblock;
        } while ($npadding); 
    }

}