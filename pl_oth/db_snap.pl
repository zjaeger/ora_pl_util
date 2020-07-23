# db_snap.pl

use DBI ;
use strict ;
use integer ;
require 'ora_connect.pl' ;

# -- BEGIN --

my ( $Lda, $c1, $index, $txt ) ;

for( $index = 1 ; $index < 11 ; ++$index )
  {
   $Lda = ora_connect('perfstat/perfstat@ddt.test.rb.cz') ;

   if( $Lda )
     {
      $c1 = $Lda->prepare("\
BEGIN
  :txt := TO_CHAR( SYSDATE,'DD.MM.YYYY HH24:MI:SS') ;
  STATSPACK.snap ;
END ;") ;

      $c1->bind_param_inout(':txt', \$txt, 30 ) ;

      if( $c1->execute() ) { print $txt ." (STATSPACK.snap)\n" ; }

      $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
      sleep 600
     }
   else
     { last ; }
  }

# -- END --

sub sysdate
  {
   my @cas   = localtime(time) ;
   my $datum = sprintf "%02d.%02d.%4d %02d:%02d:%02d",
                       $cas[3], $cas[4]+1, $cas[5]+1900,
                       $cas[2], $cas[1], $cas[0] ;
   return $datum ;
  }


sub get_uid
  {
   my ( $uname, $db_name, $db_domain ) ;
   my $c1 = $Lda->prepare("\
SELECT username,
       UPPER( SYS_CONTEXT('userenv','db_name')),
       SYS_CONTEXT('userenv','db_domain')
FROM   user_users") ;

   $c1->execute() ;
   ( $uname, $db_name, $db_domain ) = $c1->fetchrow_array() ;
   $c1->finish() ;

   return $uname .'@'. $db_name .'.'. $db_domain ;
  }
