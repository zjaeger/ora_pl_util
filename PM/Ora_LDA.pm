# Ora_LDA.pm
#
# Oracle Logon Data Area (connection handle)
#
# 2024-02-13 (last update)

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
   my $lda ;

   if( ! $userid )
     {
      if( exists $ENV{'ORA_UID'} ) { $userid = $ENV{'ORA_UID'} }
      else { die 'Error: no connect string.'."\n\n" }
     }

   $lda = DBI->connect('dbi:Oracle:', $userid,'', {
      AutoCommit => 0,
      RaiseError => 1
   } ) or die "Connect: $DBI::errstr\n" ;

   $lda->{FetchHashKeyName} = 'NAME_lc' ;

   return $lda ;
  }


sub get_uid_hash
  {
   my ( $lda ) = @_ ;
   my $rh_uid ;
   my $crs = $lda->prepare( q{
select
  a.USERNAME                               as UNAME,
  sys_context('USERENV','SERVER_HOST')     as HOST,
  upper( sys_context('USERENV','DB_NAME')) as DB_NAME,
  sys_context('USERENV','DB_DOMAIN')       as DB_DOMAIN
from
  USER_USERS a
} ) ;
   $crs->execute() ;
   $rh_uid = $crs->fetchrow_hashref() ;
   $crs->finish() ;
   if( ! defined( $rh_uid->{'db_domain'} )) { $rh_uid->{'db_domain'} = ''}

   return $rh_uid ;
  }


sub get_uid_text
  {
   my ( $lda ) = @_ ;
   my $rh_uid ;

   $rh_uid = get_uid_hash( $lda ) ;

   return $rh_uid->{'uname'} .'@//'.
          $rh_uid->{'host'} .'/'.
          $rh_uid->{'db_name'} .
          (( length( $rh_uid->{'db_domain'} ) > 0 ) ? '.' : '').
          $rh_uid->{'db_domain'} ;
  }


sub get_sysdate
  {
   my @cas = localtime(time) ;

   return sprintf "%02d.%02d.%4d %02d:%02d:%02d",
                   $cas[3], $cas[4]+1, $cas[5]+1900,
                   $cas[2], $cas[1], $cas[0] ;
  }

1;
