#!/usr/bin/perl
# sql_diff_GEN2.pl
#
# generates SQL DIFF scripts
#
# Usage:
#   sql_diff_GEN2.pl <USERID> <tab_file.csv> <col_file.csv> <fce> [<DEFAULT_OWNER>]
# where
#   <USERID>         : Oracle connect string <username>/<password>@<db_name>
#   <tab_file.csv>   : the first input CSV file - tables list
#                      header: TABLE_NAME_1;TABLE_NAME_2;Q1;Q2
#                      structure:
#                        TABLE_NAME_1: primary table name (table name only without owner)
#                        TABLE_NAME_2: optional secondary table name (<[owner.]table_name> ; if empty then TABLE_NAME_1)
#                        Q1:           optional filter condition for TABLE_NAME_1 (use alias x)
#                        Q2:           optional filter condition for TABLE_NAME_2 (use alias x)
#   <col_file.csv>   : the second input CSV file - columns list
#                      (key, added, omitted or diff columns only, possible use: key columns only)
#                      header: TABLE_NAME_1;COLUMN_NAME;COL_KOD;COL_A
#                      structure:
#                        TABLE_NAME_1: primary table name
#                        COLUMN_NAME:  column name
#                        COL_KOD:      char(1), list of values: { K: key, O: omitted, D: diff column }
#                        COL_A         char(1), list of values: { A: added col.}
#   <fce>            : generated SQL type:
#                        1 : KEY DIFF count  (differences at key columns, full outer join, uses "added columns")
#                        2 : COL DIFF count  (differences non key columns, inner join,     uses "omitted columns")
#                        3 : COL DIFF detail (differences non key columns, inner join,     uses "diff columns")
#                      notes:
#                        "key columns": mandatory, "others columns": optional
#                         fce=3 and no "diff columns" marked => uses all non key and no omitted table columns
#  [<DEFAULT_OWNER>] : optional - use only if primary tables owner differs from USERNAME at <USERID>
#  [prompt]          : optional - generate sqlplus prompt instead of comments
#
# Notes:
#   - column definitions are from database for primary table names
#     (ALL_TAB_COLUMNS, where OWNER = USERNAME or DEFAULT_OWNER)
#   - <USERID> accepts reference by environment variable (set UID_ODS=<connect_string> ; <USERID> = %UID_ODS%)
#   - tab-file and col-file needs extension exactly '.csv'
#
# Changes:
# 2017-08-02 : init
# 2019-04-25 : renamed from gen_sql_diff_PROJ.pl and redesign (input from CSV files and command line)

use strict ;
use warnings ;
use integer ;
# use Data::Dumper ;
use Ora_LDA ;
use Ora_DIFF2 ;

# constants for $rh_Tab
use constant {
  TAB2 => 0,   # table_name_2
  Q1   => 1,   # q1 (filter condition for table_name_1)
  Q2   => 2    # q2 (filter condition for table_name_1)
} ;

# -- BEGIN --

$ENV{"NLS_SORT"} = 'BINARY' ;

# if no params on command line
unless( $ARGV[ 0 ] ) { die 'No args (<userid> <file_tabs_CSV> <file_cols_CSV> <fce_no>)'."\n\n" }

# declare global variables
my ( $Lda,    # Logon Data Area (Oracle session handle)
     $Owner,  # Schema Owner
     $Fce,    # generated SQL type
     $Prompt, # (Y/N) sqlplus prompt or comment
     $rh_Tab, # { $table_name_1 } = [ $owner_1, $table_name_2, $q1, $q2 ]
     $rh_Key, # { $table_name_1 } = ( $col_1, ...)  ## columns : PK cols
     $rh_Omt, # { $table_name_1 } = ( $col_1, ...)  ## columns : ommitted cols
     $rh_Dif, # { $table_name_1 } = ( $col_1, ...)  ## columns : diff.cols (inspect)
     $rh_Add  # { $table_name_1 } = ( $col_1, ...)  ## columns : added cols to KEYs test
   ) ;

# process command line parameters
( $Lda, $Owner, $Fce, $Prompt,
  $rh_Tab, $rh_Key, $rh_Omt, $rh_Dif, $rh_Add ) = init_sql_diff_GEN( \@ARGV ) ;

if( $Lda ) # connect to Oracle is established
  {
   if( $Fce < 1 || $Fce > 3 ) { print STDERR 'Error: parameter Fce = '. $Fce .' is unexpected.'."\n" }
   elsif( ! defined $rh_Tab ) { print STDERR 'Error: no input tables.'."\n" }
   elsif( ! defined $rh_Key ) { print STDERR 'Error: no input key columns.'."\n" }
   else
     {
      my ( $tab_1, $ra_tab, $ra_cols_oth ) ;

      # set default owner
      if( $Owner ) { Ora_DIFF2::set_owner_default( $Lda, $Owner ) }  # owner = by command line
      else         { Ora_DIFF2::set_owner_default( $Lda ) }          # owner = session user

      # print header to STDOUT
      print '-- '. Ora_LDA::get_uid( $Lda ) ."\n".
            '-- '. Ora_LDA::get_sysdate() ."\n\n" ;

      # generate SQL for every input table
      for $tab_1 ( sort keys %$rh_Tab )
        {
         $ra_tab = $rh_Tab->{  $tab_1 } ;
         if( exists $rh_Key->{ $tab_1 } )
           {
            if(    $Fce == 1 )              { $ra_cols_oth = $rh_Add->{ $tab_1 } }
            elsif( $Fce == 2 || $Fce == 3 ) { $ra_cols_oth = $rh_Omt->{ $tab_1 } }

            # prints generated SQL to STDOUT
            Ora_DIFF2::print_sql_diff( $Fce,
                                       $Prompt,             # sqlplus prompt (Y/N)
                                       $Lda,                # Oracle logon data area
                                       $tab_1,              # table_1 name
                                       $ra_tab->[ TAB2 ],   # table_2 name (undef => table_1)
                                       $ra_tab->[ Q1 ],     # q_1 (filter for table_1)
                                       $ra_tab->[ Q2 ],     # q_2 (filter for table_2)
                                       $rh_Key->{ $tab_1 }, # key columns
                                       $ra_cols_oth,        # oth columns
                                       $rh_Dif->{ $tab_1 }  # dif columns (for fce=3 only)
                                     ) ;
           }
         else
           { print STDERR 'Error for table: '. $tab_1 .' - key columns not found.'."\n\n" }
        }
     }
   $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ; # Oracle disconnect
  }

# -- END --

sub header_test
  # header test for CSV file (first record $row must be the same as expected header $head)
  {
   my ( $file, $head, $row ) = @_ ;
   my $rc ;

   if( uc( substr( $row, 0, length( $head ))) eq $head ) { $rc = 0 }
   else
     {
      $rc = -1 ;
      print STDERR 'Error at file '."'". $file ."'".' - unexpected header:'."\n".
                   '  '. uc($row) ."\n".
                   '  expected: '."\n".
                   '  '.  $head ."\n" ;
     }
   return $rc ;
  }


sub read_TABS
  # read tables list from input CSV file
  {
   my ( $filename, $head ) = @_ ;
   my ( $in, $row, $cnt ) ;
   my ( $tab_1, $tab_2, $q1, $q2 ) ;
   my ( %h_tab ) ;

   open( $in, $filename ) or die 'Error on open('. $filename .'): '. $! ."\n\n" ;
   $cnt = 0 ;
   while( $row = <$in> )
     {
      $row =~ s/"//g ;
      chomp( $row ) ; $cnt += 1 ;
      if( $cnt == 1 )
        {
         if( header_test( $filename, $head, $row ) eq 0 ) { next } else { last }
        }
      if( $row )
        {
         ( $tab_1, $tab_2, $q1, $q2 ) = split(/;/, $row ) ;
         if( $tab_1 )
           {
            if( defined $h_tab{ $tab_1 } )
              { print STDERR 'Error ('. $filename .':'. $cnt .'): '. $tab_1 .'is not unique.'."\n" }
            else
              { $h_tab{ $tab_1 } = [ $tab_2, $q1, $q2 ] }
           }
         else
           { print STDERR 'Skip: '. $row ."\n" }
        }
     }
   close( $in ) ;
   return ( \%h_tab ) ;
  }


sub read_COLS
  # read columns list from input CSV file
  {
   my ( $filename, $head ) = @_ ;
   my ( $in, $row, $cnt, $tab_prv ) ;
   my ( $tab, $col, $code_KOD, $code_A, $skip_flg ) ;
   my ( @a_key, @a_omt, @a_dif, @a_add ) ;
   my ( %h_key, %h_omt, %h_dif, %h_add ) ;

   open( $in, $filename ) or die 'Error on open('. $filename .'): '. $! ."\n\n" ;
   $tab_prv = '<undef>' ;
   while( $row = <$in> )
     {
      $row =~ s/"//g ;
      chomp( $row ) ; $cnt += 1 ;
      if( $cnt == 1 )
        {
         if( header_test( $filename, $head, $row ) eq 0 ) { next } else { last }
        }
      if( $row )
        {
         ( $tab, $col, $code_KOD, $code_A ) = split(/;/, $row ) ;
         if( $tab_prv ne $tab )
           {
            if( scalar @a_key > 0 ) { $h_key{ $tab_prv } = [ @a_key ] ; @a_key = () ; }
            if( scalar @a_omt > 0 ) { $h_omt{ $tab_prv } = [ @a_omt ] ; @a_omt = () ; }
            if( scalar @a_dif > 0 ) { $h_dif{ $tab_prv } = [ @a_dif ] ; @a_dif = () ; }
            if( scalar @a_add > 0 ) { $h_add{ $tab_prv } = [ @a_add ] ; @a_add = () ; }
            $tab_prv = $tab ;
           }
         $skip_flg = 0 ;
         if( $code_KOD ) { $code_KOD = uc( $code_KOD ) }

         if(    $code_KOD eq 'K') { push( @a_key, $col ) }  # key columns
         elsif( $code_KOD eq 'O') { push( @a_omt, $col ) }  # omitted columns
         elsif( $code_KOD eq 'D') { push( @a_dif, $col ) }  # diff columns
         else
           {
            if( defined $code_KOD && $code_KOD ne '')
              { print STDERR 'Error: invalid code_KOD ('. $code_KOD .') for '. uc($tab) .'.'. lc($col) ."\n" }
            $skip_flg += 1 ;
           }

         if( defined $code_A && $code_A ne '')
           {
            if( uc( $code_A ) eq 'A')
              {
               if( $code_KOD ne 'K') { push( @a_add, $col ) ; $skip_flg -= 1 ; }  # added columns
               else { print STDERR 'Warning: code_A ('. $code_A .') for '. uc($tab) .'.'. lc($col) .' is ignored.'."\n" }
              }
            else
              { print STDERR 'Error: invalid code_A ('. $code_A .') for '. uc($tab) .'.'. lc($col) ."\n" }
           }

         if( $skip_flg > 0 ) { print STDERR 'Skip: '. $row ."\n" }
        }
     }
   close( $in ) ;
   if( scalar @a_key > 0 ) { $h_key{ $tab } = \@a_key }
   if( scalar @a_omt > 0 ) { $h_omt{ $tab } = \@a_omt }
   if( scalar @a_dif > 0 ) { $h_dif{ $tab } = \@a_dif }
   if( scalar @a_add > 0 ) { $h_add{ $tab } = \@a_add }

   return ( \%h_key, \%h_omt, \%h_dif, \%h_add ) ;
  }


sub init_sql_diff_GEN
  # process command line parameters (@ARGV) by type and order number (CVS files)
  {
   my ( $ra_argv ) = @_ ;
   my ( $lda, $owner, $fce, $prompt, $rh_tab, $rh_key, $rh_omt, $rh_dif, $rh_add ) ;
   my ( $arg, $arg_pos, $csv_no, $arg_err_FLG ) ;

   $arg_pos = $csv_no = $arg_err_FLG = 0 ;
   for $arg ( @$ra_argv )
     {
      $arg_pos++ ;
      if( $arg =~ /[\/@]/              # Oracle connect string contains "/" and "@"
          || $arg =~ /^\%[A-Z_]+\%$/ ) # shell environment variable %NAME%
        {
         unless( $lda ) { $lda = Ora_LDA::ora_LDA( $arg ) } else { $arg_err_FLG = 1 }
        }
      elsif( lc( $arg ) =~ /\.csv$/ )
        {
         $csv_no++ ;
         if( $csv_no == 1 )  # the first CSV file (tables)
           {
            $rh_tab = read_TABS( $arg,'TABLE_NAME_1;TABLE_NAME_2;Q1;Q2') ;
            # print '#TAB: '. Dumper( $rh_tab ) ;
           }
         elsif( $csv_no == 2 )  # the second CSV file (columns)
           {
            ( $rh_key, $rh_omt, $rh_dif, $rh_add ) = read_COLS( $arg,'TABLE_NAME_1;COLUMN_NAME;COL_KOD;COL_A') ;
            # print '#KEY: '. Dumper( $rh_key ) ;
            # print '#OMT: '. Dumper( $rh_omt ) ;
            # print '#DIF: '. Dumper( $rh_dif ) ;
            # print '#ADD: '. Dumper( $rh_add ) ;
           }
         else
           { $arg_err_FLG = 1 }
        }
      elsif( $arg =~ /^\d+$/ ) # number
        {
         unless( defined $fce ) { $fce = $arg } else { $arg_err_FLG = 1 }
        }
      elsif( uc($arg) eq 'PROMPT')
        {
         unless( defined $prompt ) { $prompt = 1 } else { $arg_err_FLG = 1 }
        }
      elsif( $arg =~ /^[A-Za-z]+/ ) # string -> default owner
        {
         unless( defined $owner ) { $owner = $arg } else { $arg_err_FLG = 1 }
        }
      else
        { $arg_err_FLG = 1 }

      if( $arg_err_FLG )
        {
         $arg_err_FLG = 0 ;
         print STDERR 'Error on '. $arg_pos .'. argument: '. $arg .': unrecognized.'."\n" ;
        }
     }

   if( ! $fce )    { $fce = 1 }
   if( ! $prompt ) { $prompt = 0 }
   return ( $lda, $owner, $fce, $prompt, $rh_tab, $rh_key, $rh_omt, $rh_dif, $rh_add ) ;
  }
