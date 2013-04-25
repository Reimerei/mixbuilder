<?php
//get connection
$con = mysql_connect("fdb3.awardspace.com","558585_mix","shifter");
if (!$con)
  {
  die('Could not connect: ' . mysql_error());
  }

//operations
mysql_select_db("558585_mix", $con);

insertNewMix($con, $_POST[from], $_POST[to]);

function insertNewMix($con, $in_from, $in_to){
//insert new mix record into db
	if(!mysql_query("insert into mixes values ($in_from, $in_to)",$con))
	{
	die('Error: ' . mysql_error());
	}
	echo "mix added";
}



//close connection
mysql_close($con);
?> 


</body>
</html> 