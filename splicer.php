<?php
    
    //=============================================================================
    /* 
     Splice
     
     This script does three things.
     
         1) Splices together multiple feeds at user-specified start-points.
         2) Converts all source feeds from PHPFina or PHPFiwa to a target of either format.
         3) Resamples all source feeds at a user-specified common-denominator interval.

    */
    
    $low_memory_mode = false;
    define('EMONCMS_EXEC', 1);
    require "Lib/PHPFina.php";
    require "Lib/PHPFiwa.php";
    require "Lib/PHPTimeSeries.php";
    require "Lib/EmonLogger.php";
    
    $userid = 1; //modify if different userid is needed
    
    chdir("/var/www/html/emoncms"); //some installations use /var/www/emoncms, some /var/www/emoncms
    
    if (!file_exists("process_settings.php")) {
        echo "ERROR: This is not a valid emoncms directory, please retry\n"; die;
    }
    // Load emoncms install
    require "process_settings.php";
    
    $mysqli = @new mysqli($server,$username,$password,$database);
    
    if (class_exists('Redis') && $redis_enabled) {
        $redis = new Redis();
        $connected = $redis->connect("127.0.0.1");
        if (!$connected) {
            echo "Can't connect to redis database, it may be that redis-server is not installed or started see readme for redis installation"; die;
        }
    } else {
        $redis = false;
    }
    
    $engine = array();
    $engine[Engine::PHPFINA] = new PHPFina($feed_settings['phpfina']);
    $engine[Engine::PHPFIWA] = new PHPFiwa($feed_settings['phpfiwa']);
    
    $result = $mysqli->query("SELECT * FROM feeds WHERE `engine` IN ('5','6')");
    while($row = mysqli_fetch_assoc($result))
    {
        echo $row["id"];
        echo "\n";
        
    }
    
    //=============================================================================
	// Souce feed selection
      
    $source = 0;
    $n = 1;
    $feeds = array();
	$source_engines = array();
	$source_startpoints = array();
    $startpoints = array();
    $intervals= array();
    
    do { //while(!empty($source));
        $source=stdin("Please enter feed ID {$n} or press return to continue: ");
        $result = $mysqli->query("SELECT * FROM feeds WHERE id='".$source."'");
        
        if (empty($source)) {break;}
        
        if (!$result->num_rows) {
            echo "ERROR: Feed does not exist\n";
            continue;
        }
        
        $row = $result->fetch_array();
        if ($row["engine"]!=5 && $row["engine"]!=6) {
            ECHO "ERROR: Please choose a PHPFina or PHPFiwa feed\n";
            continue;
            }
        
        $feeds[$n] = $source;
        
        $source_engines[$n] = $row["engine"];
		
        if ($row["engine"]==Engine::PHPFINA) $sourcemeta = $engine[Engine::PHPFINA]->get_meta($source);
        if ($row["engine"]==Engine::PHPFIWA) $sourcemeta = $engine[Engine::PHPFIWA]->get_meta($source);
        
        $startpoints[$n] = $sourcemeta->start_time;
		$source_startpoints[$n] = $sourcemeta->start_time;
        echo ("Feed start time (default): {$startpoints[$n]}\n");
		
        
		if ($source_engines[$n]==5) $intervals[$n] = $sourcemeta->interval;
        if ($source_engines[$n]==6) $intervals[$n] = $sourcemeta->interval[0];    //use the base layer of PHPFiwa feeds
		
		
		
        do { //while empty($startselect)
            $startselect=stdin("Enter a start point or press enter: ");
             
 			if (empty($startselect) && $n > 1 && $startpoints[$n] < $startpoints[$n-1]) {
				echo("\nERROR: Each startpoint must be later than the once for the previous feed\n");
				continue;
			}
			
			if (empty($startselect)) break;
			
            if (!is_numeric($startselect)) {
                echo("\nERROR: Must be numberic\n");
				$startselect="";
                continue;
            }
			
            if ($startselect < $startpoints[$n]) {
                echo("\nERROR: Must be greater than the feed start time\n");
				$startselect="";
                continue;
            }
			
			if ($n > 1 && $startselect < $startpoints[$n-1]) {
				echo("\nERROR: Each startpoint must be later than the once for the previous feed\n");
				$startselect="";
				continue;
			}

            $startpoints[$n] = $startselect;
            
        } while (empty($startselect));
		
        $n++;
        
    } while(!empty($source));
    
    if (empty($feeds)) die;
    
    if ($source_engines[$n-1]==Engine::PHPFINA) $lastfeednbytes = filesize("/var/lib/phpfina/".$feeds[$n-1].".dat");
    if ($source_engines[$n-1]==Engine::PHPFIWA) $lastfeednbytes = filesize("/var/lib/phpfiwa/".$feeds[$n-1]."_0.dat");
    $lastfeednpoints = floor($lastfeednbytes) / 4.0;
    $endpoint = $startpoints[$n-1] + $intervals[$n-1] * $lastfeednpoints;
	
	
    
    //=============================================================================
	// Get the engine for the new feed 
    echo "\n";
	do { //while empty($target_engine)
		$target_engine=stdin("\nSelect the engine for the target feed\n 5. PHPFina\n 6. PHPFiwa\n Selection: ");
		if ($target_engine!=5 && $target_engine!=6) {
		echo("Please enter the number of your choice.\n\n");
		$target_engine="";
		continue;
		}
	} while (empty($target_engine));
	
	
	//=============================================================================
	// Get the interval for the new feed
	
	$max_interval = max($intervals);
	
	do { //while empty($target_interval)
		echo ("\nSelect the interval for the target feed.\n");
		echo ("5. 5 sec\n".
		      "10. 10 sec \n".
			  "15. 15 sec \n".
			  "20. 20 sec \n".
			  "30. 30 sec \n".
			  "60. 1 min \n".
			  "120. 2 min \n".
			  "300. 5 min \n".
			  "600. 10 min \n".
			  "1200. 20 min \n".
			  "1800. 30 min \n".
			  "3600. 1 hr \n");
		
		$target_interval=stdin("Seconds: ");
		
		if ($target_interval > $max_interval) {
			echo "ERROR: The maximum interval is the smallest feed interval, which is ".$max_interval.".";
			$target_interval = "";
			continue;
        } else {
        
            foreach($intervals as $i) {
                if ($i % $target_interval <> 0) {
                    echo "ERROR: The target interval must evenly divde the source intervals.\nYour choice does not divide ".$i.".";
                    $target_interval = "";
                    continue 2;
                }
            }
        }
        
        if (in_array($target_interval, array(5, 10, 15, 20, 30, 60, 120, 300, 600, 1200, 1800, 3600))) break;
    
		$target_interval="";
		echo("Please select the number of seconds for the target interval.\n");
	} while (empty($target_interval));
	
	//=============================================================================
	//Display the source start points and commit to continue
	
	
	foreach  ($feeds as $n => $feedid) {
		echo "Feed: ", $n,"    Source: ", $feeds[$n],"  From: ",$startpoints[$n];
		if (isset($startpoints[$n+1])) {
			echo "  To: ", $startpoints[$n+1]-1, "\n";
		}
	}
	echo "  To: ",$endpoint,"\n";
	
	$go="";
	while ($go!="Y" && $go!="y" && $go!="N" && $go!="n") {
		$go=stdin("\nCreate Feed? (y/n): ");
	}
	if ($go=="n" || $go=="N") die;
	
	//=============================================================================
	// Create the new feed
    $target = 0;
    
    $new_feed = $row['name']." Spliced";
    $datatype = DataType::REALTIME;
    
    $result = $mysqli->query("INSERT INTO feeds (userid,name,datatype,public,engine) VALUES ('$userid','$new_feed','$datatype',false,'$target_engine')");
    $target = $mysqli->insert_id;
   
    
    // force a reload of the feeds table
    if ($redis && $redis->exists("user:feeds:$userid")) {
        $redis->del("user:feeds:$userid");
        $redis->del("feed:lastvalue:$target");
    }
    
    if ($target==0) {
		echo("\nError inserting feed into MySQL database.");
		die;
	}
	
    if ($target_engine==5) $engine[Engine::PHPFINA]->create($target,array("interval"=>$target_interval));
    if ($target_engine==6) $engine[Engine::PHPFIWA]->create($target,array("interval"=>$target_interval));
	
    echo "\n";
    echo "Target feed id : $target\n";
	echo "Target interval: $target_interval\n";

    $go="";
    while ($go!="Y" && $go!="y" && $go!="N" && $go!="n") {
        $go=stdin("\nStart splicing? (y/n): ");
    }
    if ($go=="n" || $go=="N") die;
   
    
	//=============================================================================
	//splice, resample, convert
    
    $timepos = $startpoints[1]; //the main timekeeper for the target feed. Start it at the earliest chosen startpoint.
       
	foreach ($feeds as $n => $feedid) {
		
		if (isset($startpoints[$n+1])) $feedendpoint = $startpoints[$n+1]; else $feedendpoint = $endpoint;
	
        $sourcetimepos = $source_startpoints[$n]; //the timekeeper for the current source feed. Start it at the earliest point in the source feed.
		
        $ratio = $intervals[$n] / $target_interval;
		$skips = $ratio - 1;
		
		while ($dp = $engine[$source_engines[$n]]->readnext($feeds[$n]))
        {
            if ($timepos >= $feedendpoint) break;
            
            //skip datapoints while we read up to the chosen startpoint of the current feed
            if ($sourcetimepos < $startpoints[$n]) {
                $sourcetimepos += $intervals[$n];
                continue;
                
			} else {
                $tm=$dp['time'];
				$val=$dp['value']; //if we are at or after the chosen start point of the currect source feed we can write the value
			}
				
            if ($target_engine==Engine::PHPFINA) $engine[$target_engine]->prepare($target,$tm,$val);
            if ($target_engine==Engine::PHPFIWA) $engine[$target_engine]->post($target,$tm,$val);
            
			if ($low_memory_mode && $target_engine==Engine::PHPFINA) $engine[$target_engine]->save();
			
            $timepos += $target_interval;
			$sourcetimepos += $intervals[$n];
            			
			for ($s=0; $s < $skips; $s++) {
				$timepos += $target_interval;
			}
            
		}
        
	}
    if ($target_engine==Engine::PHPFINA) $engine[Engine::PHPFINA]->save();
		
	$mysqli->query("UPDATE feeds SET value= '".$val."' WHERE id='".$target."'");
	
    if ($target_engine==5) {
        exec("chown www-data:www-data ".$feed_settings['phpfina']['datadir'].$target.".meta");
        exec("chown www-data:www-data ".$feed_settings['phpfina']['datadir'].$target.".dat");
    }
	
    if ($target_engine==6) {
        exec("chown www-data:www-data ".$feed_settings['phpfiwa']['datadir'].$target.".meta");
		$targetmeta = $engine[Engine::PHPFIWA]->get_meta($target);
        for ($i=0; $i<$targetmeta->nlayers; $i++)
        {
            exec("chown www-data:www-data ".$feed_settings['phpfiwa']['datadir'].$target."_$i.dat");
        }
        
    }
    
    // force a reload of the feeds table
    if ($redis && $redis->exists("user:feeds:$userid")) {
        $redis->del("user:feeds:$userid");
        $redis->del("feed:lastvalue:$target");
    }
	
	echo "\n";
    echo "Wrote to target feed id : $target\n"; 
    
    function stdin($prompt = null){
        if($prompt){
            echo $prompt;
        }
        $fp = fopen("php://stdin","r");
        $line = rtrim(fgets($fp, 1024));
        return $line;
    }
        
    
     