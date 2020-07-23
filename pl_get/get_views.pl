#!/usr/bin/perl
# get_views.pl
#
# SCHEMA VIEWS UPLOAD (into text-files):
#
# USAGE: get_views.pl <oracle_connect_string> [<view_name>]
# (oracle_connect_string: username/password@db_name)

use strict ;
use warnings ;
use integer ;
use DBI ;
use FileHandle ;
use Ora_LDA ;

# -- BEGIN --

$ENV{"NLS_SORT"} = 'BINARY' ;

unless( $ARGV[ 0 ] ) { die "No args ( <connect_string> [ view_name ] ).\n\n" ; }

my $Lda = Ora_LDA::ora_LDA( $ARGV[ 0 ] ) ;

if( $Lda )
  {
   $Lda->{LongReadLen} = 16384 ;

   save_label() ;

   upload_views( $ARGV[ 1 ] ) ;

   $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
  }

# -- END --

sub save_label
  {
   my ( $sysdate, $uid, $fname ) ;

   $sysdate = Ora_LDA::get_sysdate() ;
   $uid     = Ora_LDA::get_uid( $Lda ) ;

   $fname = '00_db_views.lst' ;

   open( OUT, ">$fname") || die "Can't open file ". $fname ."\n\n" ;
   print OUT 'Date:   '. $sysdate ."\n".
             'Schema: '. $uid ."\n" ;
   close( OUT ) ;

   print 'File '. $fname .' created.'."\n" ;
  }


sub upload_table_3  # -- table and column comments
  {
   my ( $fh, $table_name ) = @_ ;
   my ( $c_tc1, $c_tc2 ) ;
   my ( $type, $col_name, $text ) ;

   $c_tc1 = $Lda->prepare("
SELECT table_type, comments
FROM   user_tab_comments
WHERE  table_name = ?
       AND comments IS NOT NULL") ;

   $c_tc1->execute( $table_name ) ;
   ( $type, $text ) = $c_tc1->fetchrow_array() ;
   $c_tc1->finish() ;

   if( $type )
     {
      $text =~ s/\'/\'\'/g ;
      print $fh 'COMMENT ON '. $type .' '. $table_name .' IS'."\n".
                "\'". $text ."\'"."\n".
                "/\n" ;
     }

   $c_tc2 = $Lda->prepare("
SELECT column_name, comments
FROM   user_col_comments
WHERE  table_name = ?
       AND comments IS NOT NULL
ORDER BY column_name") ;

   $c_tc2->execute( $table_name ) ;
   while( ( $col_name, $text ) = $c_tc2->fetchrow_array() )
     {
      $text =~ s/\'/\'\'/g ;
      print $fh 'COMMENT ON COLUMN '. $table_name .'.'. $col_name .' IS'."\n".
                "\'". $text ."\'"."\n".
                "/\n" ;
     }
  }


sub upload_tab_triggers
  {
   my ( $table_name, $table_type ) = @_ ;
   my ( $trg_name, $row_no ) ;
   my $fname ;
   my $text ;

   my $c_trg = $Lda->prepare("\
SELECT
  trigger_name,
  ROW_NUMBER() OVER (ORDER BY trigger_name )  AS row_no
FROM
  user_triggers
WHERE
  table_name = ?
  AND base_object_type = ?
ORDER BY
  trigger_name") ;

   my $c_src = $Lda->prepare("\
SELECT RTRIM( text ) FROM user_source WHERE name = ? AND type = ?
ORDER BY line") ;
  
   $c_trg->execute( $table_name, $table_type ) ;

   while( ( $trg_name, $row_no ) = $c_trg->fetchrow_array() )
     {
      if( $row_no == 1 )
        {
         $fname = lc( $table_name ) .'_TR.pls' ;
         open( OUT, '>'. $fname ) or die "Can't open file: ". $fname ."\n\n" ;
         print OUT '-- '. $fname ."\n\n" ;
        }

      print OUT 'PROMPT Trigger '. uc( $trg_name ) ."\n\n" ;

      print OUT 'CREATE OR REPLACE ' ;
      $c_src->execute( $trg_name,'TRIGGER') ;
      while( ( $text ) = $c_src->fetchrow_array() ) { print OUT $text }

      print OUT "/\n\n" ;
     }

   if( $c_trg->rows > 0 )
     {
      close( OUT ) ;
      print 'File '. $fname .' created.'."\n" ;
     }
  }


sub upload_views
  {
   my ( $view_name_IN ) = @_ ;
   my ( $view_name, $text, $cnt_t, $col_name ) ;
   my ( $fname, $fh, $sep ) ;
   my ( $len, $line_no ) ;
   my ( $row, @Rows ) ;
   my $c_vtxt ;

   if( defined( $view_name_IN ))
     {
      $text = "\n".'WHERE'."\n".
                   '  A.view_name LIKE '."'". $view_name_IN ."'\n" ;
     }
   else
     { $text = '' }

   $c_vtxt = $Lda->prepare("\
SELECT
  A.view_name,
  A.text,
  ( SELECT COUNT( B.trigger_name ) 
    FROM   user_triggers B
    WHERE  B.table_name = A.view_name ) AS cnt_t
FROM
  user_views A". $text . "\
ORDER BY
  A.view_name") ;

   my $c_vcol = $Lda->prepare("\
SELECT LOWER( column_name )
FROM   user_tab_columns
WHERE  table_name = ?
ORDER BY column_id") ;

   $fh = new FileHandle ;
   $c_vtxt->execute() ;

   while( ( $view_name, $text, $cnt_t ) = $c_vtxt->fetchrow_array() )
     {
      $fname = lc( $view_name ) .'_VW.sql' ;
      $fh->open('>'. $fname ) or die "Can't open file ". $fname ."\n\n" ;
      print $fh '-- '. $fname ."\n\n" ;

      print $fh 'PROMPT View '. uc( $view_name ) ."\n\n" ;

      print $fh 'CREATE OR REPLACE FORCE VIEW '. uc( $view_name ) ."(\nAS" ;

=pod
      $c_vcol->execute( $view_name ) ;
      $len = 0 ; $line_no = 1 ; $sep = '/*01*/ ' ;
      while( ( $col_name ) = $c_vcol->fetchrow_array() )
        {
         print $fh $sep . $col_name ;

         $len += length( $col_name ) + 2 ;

         if( $len < 50 )
           { $sep = ', ' ; }
         else
           {
            ++$line_no ; $len = 0 ;
            if( $line_no & 0x01 )
              { $sep = sprintf(",\n/*%02d*/ ", $line_no ) ; }
            else
              { $sep = ",\n".'       ' ; }
           }
        }
      print $fh ' )'."\n".'AS ' ;
=cut

      @Rows = split(/[\n]/, $text ) ;
      foreach $row ( @Rows )
        {
         $row =~ s/[\s]+$// ;
         if( length( $row ) > 0 ) { print $fh "\n". $row }
        }
      print $fh ";\n" ;
      ## print $fh $text ."\n/\n" ;  ## problem s praznym radkem na konci (Ora11) a pod.

      # -- comments

      upload_table_3( $fh, $view_name ) ;

      # -- triggers (into other file)

      if( $cnt_t > 0 )  { upload_tab_triggers( $view_name,'VIEW') }

      $fh->close() ;
      print 'File '. $fname .' created.'."\n" ;
     }
  }

# --- End of get_views.pl
