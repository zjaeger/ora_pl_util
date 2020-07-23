# ora_LDA.pm
#
# 2017-07-31

BEGIN {
   $ENV{'ORACLE_HOME'} = 'D:\x\ora_xe\app\oracle\product\11.2.0\server' ;
## $ENV{'NLS_LANG'} = 'AMERICAN_AMERICA.EE8MSWIN1250' ;
## $ENV{'NLS_NUMERIC_CHARACTERS'} = '. ' ;
}

package Ora_LDA ;

use strict ;
use warnings ;
use DBI ;
use Exporter ;

@Ora_LDA::ISA    = qw(Exporter) ;
@Ora_LDA::EXPORT = qw(&ora_LDA &get_uid &get_sysdate) ;


sub ora_LDA
  {
   my ( $value, $verbose ) = @_ ;
   my ( $userid, $usr, $pass, $sid, $lda ) ;

   if( ! $value ) { $value = 'ORA_UID' }

   if( $value =~ /^\w+$/ )
     {
      if( exists $ENV{ $value } ) { $userid = $ENV{ $value } }
      else { print STDERR 'Error: '. $value .' - invalid environment variable for connect string.'."\n" }
     }
   else
     { $userid = $value }

   if( $userid )
     {
      ( $usr, $pass, $sid ) = $userid =~ m/([\w]+)\/([\S]+)@([\w\.]+)/ ;

      if( defined $sid )
        {
         if( defined( $verbose ) ) { print 'connect '. lc( $usr ) .'@'. uc( $sid ) ."\n" }

         $lda = DBI->connect("dbi:Oracle:$sid", $usr, $pass, {
            AutoCommit => 0,
            RaiseError => 1
         } ) or die "Connect: $DBI::errstr\n" ;
        }
      else
        { print STDERR 'Error: invalid Oracle connect string: '. $userid ."\n" }
     }

   return $lda ;
  }


sub get_uid
  {
   my ( $lda ) = @_ ;
   my ( $uname, $db_name, $db_domain ) ;
   my $crs = $lda->prepare("\
SELECT username,
       UPPER( SYS_CONTEXT('userenv','db_name')),
       SYS_CONTEXT('userenv','db_domain')
FROM   user_users") ;

   $crs->execute() ;
   ( $uname, $db_name, $db_domain ) = $crs->fetchrow_array() ;
   $crs->finish() ;

   return $uname .'@'. $db_name . ((defined $db_domain) ? '.'. $db_domain : '') ;
  }


sub get_sysdate
  {
   my @cas = localtime(time) ;

   return sprintf "%02d.%02d.%4d %02d:%02d:%02d",
                   $cas[3], $cas[4]+1, $cas[5]+1900,
                   $cas[2], $cas[1], $cas[0] ;
  }

1;
