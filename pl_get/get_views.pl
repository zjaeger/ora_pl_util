#!/usr/bin/perl
#
# get_views.pl
#
# SCHEMA VIEWS UPLOAD (into text-files):
#
# USAGE: get_views.pl <oracle_connect_string> [ <view_name_REGEXP>|all ] ...
# (oracle_connect_string: username/password@db_name)
#
# 2024-02-02 (last update)

use strict ;
use warnings ;
use integer ;
use File::Basename ;
use DBI ;
use Ora_LDA ;

# -- BEGIN --

$ENV{"NLS_SORT"} = 'BINARY' ;

my ( $Userid, $ra_Names ) = get_input_params( \@ARGV ) ;
my $Lda ;

# break flag variable
my $Sig_break = 0 ; $SIG{INT} = sub { $Sig_break = 1 } ;

# connect to Oracle DB
if( $Lda = Ora_LDA::ora_LDA( $Userid ))
  {
   $Lda->{LongReadLen} = 16384*4 ;
   eval
     {
      save_label('00_db_views.lst') ;

      for my $view_name_RE ( @$ra_Names ) { upload_views( $view_name_RE ) }
     } ;
   if( $@ ) { print STDERR $@ ."\n" }

   # Oracle disconnect
   $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
  }

# -- END --

sub get_input_params
  {
   my ( $ra_arg ) = @_ ;
   my ( $userid, # Oracle connect string
        @a_name  # view name regexp values
      ) ;

   @a_name = () ;
   foreach my $val ( @$ra_arg )
     {
      if( $val =~ /^[\w]+\/[\S]+@[\w\.]+$/
          || ( $val =~ /^\w+$/ && exists $ENV{ $val } ) )
        {
         if( ! defined( $userid )) { $userid = $val }
         else
           { print STDERR 'Unexpected value (userid): '. $val ."\n" }
        }
      else
        { push( @a_name, $val ) }
     }

   if( scalar @a_name == 0 )  { push( @a_name,'.*') }

   unless(    defined( $userid )
           && scalar @a_name != 0 )
     {
      if( scalar @$ra_arg > 0 ) { print STDERR 'Invalid parameters'."\n" }
      printf STDERR 'Usage: '. basename( $0 ) .' <userid> [<view_name_RE>] ...'."\n\n" ;
      exit 1 ;
     }

   return ( $userid, \@a_name ) ;
  }


sub save_label
  {
   my ( $fname ) = @_ ;
   my ( $sysdate, $uid, $out ) ;

   $sysdate = Ora_LDA::get_sysdate() ;
   $uid     = Ora_LDA::get_uid( $Lda ) ;

   open( $out,'>', $fname ) || die 'Error on open('. $fname .'): '. $! ."\n\n" ;
   print $out 'Date:   '. $sysdate ."\n".
              'Schema: '. $uid ."\n" ;
   close( $out ) ;

   print 'File '. $fname .' created.'."\n" ;
  }


sub upload_tab_comments  # -- table and column comments
  {
   my ( $out, $table_name ) = @_ ;
   my ( $c_tc1, $c_tc2 ) ;
   my ( $type, $col_name, $text ) ;

   $c_tc1 = $Lda->prepare( q{
select a.TABLE_TYPE, a.COMMENTS
from   USER_TAB_COMMENTS a
where  a.TABLE_NAME = ? and a.COMMENTS is not null
} ) ;
   $c_tc1->execute( $table_name ) ;
   ( $type, $text ) = $c_tc1->fetchrow_array() ;
   $c_tc1->finish() ;

   if( $type )
     {
      $text =~ s/\'/\'\'/g ;
      print $out 'COMMENT ON '. $type .' '. $table_name .' IS'."\n".
                 "\'". $text ."\'"."\n".
                 "/\n" ;
     }

   $c_tc2 = $Lda->prepare( q{
select a.COLUMN_NAME, a.COMMENTS
from   USER_COL_COMMENTS a
where  a.TABLE_NAME = ? and a.COMMENTS is not null
order by a.COLUMN_NAME
} ) ;
   $c_tc2->execute( $table_name ) ;
   while( ( $col_name, $text ) = $c_tc2->fetchrow_array() )
     {
      $text =~ s/\'/\'\'/g ;
      print $out 'COMMENT ON COLUMN '. $table_name .'.'. $col_name .' IS'."\n".
                 "\'". $text ."\'"."\n".
                 "/\n" ;
     }
  }


sub upload_tab_triggers
  {
   my ( $table_name, $table_type ) = @_ ;
   my ( $trg_name, $row_no, $fname, $text, $out ) ;

   my $c_trg = $Lda->prepare( q{
select
  a.TRIGGER_NAME,
  row_number() over (order by a.TRIGGER_NAME ) as ROW_NO
from
  USER_TRIGGERS a
where
  a.TABLE_NAME = ?
  and a.BASE_OBJECT_TYPE = ?
order by
  a.TRIGGER_NAME
} ) ;

   my $c_src = $Lda->prepare( q{
select rtrim( a.TEXT ) from USER_SOURCE a where a.NAME = ? and a.TYPE = ? order by a.LINE
} ) ;

   $c_trg->execute( $table_name, $table_type ) ;

   while( ( $trg_name, $row_no ) = $c_trg->fetchrow_array() )
     {
      if( $row_no == 1 )
        {
         $fname = lc( $table_name ) .'_TR.pls' ;
         open( $out,'>'. $fname ) || die 'Error on open ('. $fname .'): '. $! ."\n\n" ;
         print $out '-- '. $fname ."\n\n" ;
        }

    # print $out 'prompt >>> create trigger '. uc( $trg_name ) ."\n\n" ;

      print $out 'CREATE OR REPLACE ' ;
      $c_src->execute( $trg_name,'TRIGGER') ;
      while( ( $text ) = $c_src->fetchrow_array() ) { print $out $text }

      print $out "\n/\n" ;
     }

   if( $c_trg->rows > 0 )
     {
      close( $out ) ; print 'File '. $fname .' created.'."\n" ;
     }
  }


sub upload_views
  {
   my ( $view_name_IN ) = @_ ;
   my ( $view_name, $text, $cnt_t ) ;
   my ( $fname, $out, $in, $row ) ;

   my $c_vie = $Lda->prepare( q{
select
  a.VIEW_NAME,
  a.TEXT,
  ( select count( b.TRIGGER_NAME )
    from   USER_TRIGGERS b
    where  b.TABLE_NAME = a.VIEW_NAME ) as CNT_T
from
  USER_VIEWS a
where
  regexp_like( a.VIEW_NAME, ?,'i')
order by
  a.VIEW_NAME
} ) ;

   $c_vie->execute( $view_name_IN ) ;

   while( ( $view_name, $text, $cnt_t ) = $c_vie->fetchrow_array() )
     {
      $fname = lc( $view_name ) .'.sql' ;
      open( $out,'>'. $fname ) || die 'Error on open('. $fname .'): '. $! ."\n\n" ;
      print $out '-- '. $fname ."\n".
                 '--'."\n".
                 '-- rdbms: oracle'."\n\n" ;

    # print $out 'prompt >>> create view '. uc( $view_name ) ."\n\n" ;
      print $out 'CREATE OR REPLACE VIEW '. lc( $view_name ) ."\n".
                 'AS' ;

      open( $in,'<', \$text ) ;
      while( defined( $row = <$in> ) )
        {
         $row =~ s/[\s]+$// ; if( $row ) { print $out "\n". $row }
        }
      close( $in ) ;
      print $out "\n".'/'."\n" ;

      # -- comments
      upload_tab_comments( $out, $view_name ) ;

      close( $out ) ; print 'File '. $fname .' created.'."\n" ;

      # -- triggers (into other file)
      if( $cnt_t > 0 ) { upload_tab_triggers( $view_name,'VIEW') }

      if( $Sig_break != 0 ) { print 'BREAK'."\n" ; $c_vie->finish() ; last ; }
     }

   if( ! defined $fname )
     { print 'Warning: no data found for view_name_REGEXP='. $view_name_IN ."\n" }
  }

# --- End of get_views.pl
