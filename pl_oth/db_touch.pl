#!/usr/bin/perl
# db_touch.pl
#
# connection loop test only

use strict ;
use warnings ;
use DBI ;
use Ora_LDA ;

# -- BEGIN --

my ( $Lda, $crs, $index, $txt, $sysd ) ;
my $cnt = $ARGV[ 1 ] ;

unless( $ARGV[ 0 ] ) { die "No args (connect string and loop count expected).\n\n" ; }
unless( $cnt ) { $cnt = 100 }

for( $index = 0 ; $index < $cnt ; ++$index )
  {
   $Lda = Ora_LDA::ora_LDA( $ARGV[ 0 ] ) ;

   if( $Lda )
     {
      $crs = $Lda->prepare("\
BEGIN
  :txt := TO_CHAR( SYSDATE,'DD.MM.YYYY HH24:MI:SS') ;
END ;") ;

      $crs->bind_param_inout(':txt', \$txt, 30 ) ;

      $sysd = Ora_LDA::get_sysdate() ;
      if( $crs->execute() )
        {
         print Ora_LDA::get_uid( $Lda ) .
               ', db-sysdate: '. $txt .
               ', os_sysdate: '. $sysd ."\n" ;
        }

      $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
      print '>>> sleep 1200'."\n" ;
      sleep 1200
     }
   else
     { last ; }
  }

# -- END --
