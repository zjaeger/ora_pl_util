# Ora_LDA.pm
#
# Oracle Logon Data Area (connection handle)
#
# 2024-02-02 (last update)

package Ora_LDA ;

use strict ;
use warnings ;
use DBI ;
# use Exporter ;

# @Ora_cob::ISA    = qw(Exporter) ;
# @Ora_cob::EXPORT = qw(&ora_LDA &get_uid) ;

sub ora_LDA
  {
   my ( $userid ) = @_ ;
   my ( $usr, $pass, $sid, $lda ) ;

   if( ! $userid )
     {
      if( exists $ENV{'ORA_UID'} ) { $userid = $ENV{'ORA_UID'} }
      else { die 'Error: no connect string.'."\n\n" }
     }

   ( $usr, $pass, $sid ) = $userid =~ m/([\w]+)\/([\S]+)@([\w\.]+)/ ;

   if( ! defined $usr ) { die 'Error: invalid connect string: '. $userid .".\n\n" }

   # printf "Oracle connect (%s/%s@%s) ...\n", $usr, $pass, $sid ;

   $lda = DBI->connect("dbi:Oracle:$sid", $usr, $pass, {
      AutoCommit => 0,
      RaiseError => 1
   } ) or die "Connect: $DBI::errstr\n" ;

   return $lda ;
  }


sub get_uid
  {
   my ( $lda ) = @_ ;
   my ( $uname, $db_name, $db_domain ) ;
   my $crs = $lda->prepare("\
select a.USERNAME,
       upper( sys_context('USERENV','DB_NAME')),
       sys_context('USERENV','DB_DOMAIN')
from   USER_USERS a") ;

   $crs->execute() ;
   ( $uname, $db_name, $db_domain ) = $crs->fetchrow_array() ;
   $crs->finish() ;

   return $uname .'@'. $db_name . ((defined $db_domain ) ? '.'. $db_domain : '') ;
  }


sub get_sysdate
  {
   my @cas = localtime(time) ;

   return sprintf "%02d.%02d.%4d %02d:%02d:%02d",
                   $cas[3], $cas[4]+1, $cas[5]+1900,
                   $cas[2], $cas[1], $cas[0] ;
  }

1;
