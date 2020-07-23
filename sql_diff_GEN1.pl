#!/usr/bin/perl
# sql_diff_GEN1.pl
#
# 2019-04-28
#
# generates SQL DIFF scripts for single table (input by cmd-line or by parfile)

use strict ;
use warnings ;
use integer ;
use Ora_UTL_INPUT ;
use Ora_LDA ;
use Ora_DIFF2 ;

# input parameters (from cmd-line or from parameters file)
my ( $userid,
     $fce,
     $prompt_flg,
     $tab1,
     $tab2,
     $q1,
     $q2,
     $ra_cols_key_in,
     $ra_cols_add_in,
     $ra_cols_dif_in,
     $ra_cols_omt_in
   ) ;

# input parameters description and reference to target variable
# (description - constants for @a_params before)
my @a_params = (
  ['userid',  'uid',  's','m',\$userid,        'Oracle connect string'],
  ['table1',  'tab1', 's','m',\$tab1,          'Primary table name <[owner.]table_name>'],
  ['table2',  'tab2', 's','o',\$tab2,          'Secondary table name <[owner.]table_name>'],
  ['query1',  'q1',   's','o',\$q1,            'optional where clause for primary table name'],
  ['query2',  'q2',   's','o',\$q2,            'optional where clause for secondary table name'],
  ['cols_key','c_key','a','m',\$ra_cols_key_in,'unique columns (for join clause)'],
  ['cols_add','c_add','a','o',\$ra_cols_add_in,'added columns to KEY DIFF CNT sql (fce=1)'],
  ['cols_omt','c_omt','a','o',\$ra_cols_omt_in,'omitted columns (fce=2,3)'],
  ['cols_dif','c_dif','a','o',\$ra_cols_dif_in,'detail diff. columns (fce=3)'],
  ['fce',     undef,  's','m',\$fce,           'generated SQL type: 1 - KEY DIFF CNT, 2 - COLS DIFF CNT, 3 - COLS DIFF DETAIL'],
  ['prompt',  undef,  's','o',\$prompt_flg,    'if true (!=0) generate sqlplus prompt command instead of comment']
) ;

# --- BEGIN ---
if( scalar @ARGV == 0 )
  {
   print STDERR 'Generate SQL DIFF script for two tables with the same structure (structure by primary table).'."\n".
                'Input parameters description:'."\n".
                'cmd> '. Ora_UTL_INPUT::basename($0) .' help=Y'."\n\n" ;
  }
elsif( Ora_UTL_INPUT::read_inputs( \@ARGV, \@a_params ) == 0 )
  {
   # print '>>> tab1 = '. $tab1 ."\n" ;
   # print '>>> tab1 = '. $tab1 ."\n" ;
   # print '>>> q1 = '. $q1 ."\n" ;
   # print '>>> q2 = '. $q2 ."\n" ;
   # print '>>> cols_key = '. ((defined $ra_cols_key_in) ? join(', ', @$ra_cols_key_in ) : '') ."\n" ;
   # print '>>> cols_add = '. ((defined $ra_cols_add_in) ? join(', ', @$ra_cols_add_in ) : '') ."\n" ;
   # print '>>> cols_omt = '. ((defined $ra_cols_omt_in) ? join(', ', @$ra_cols_omt_in ) : '') ."\n" ;
   # print '>>> cols_dif = '. ((defined $ra_cols_dif_in) ? join(', ', @$ra_cols_dif_in ) : '') ."\n" ;
   # print '>>> fce      = '. $fce ."\n" ;
   # print '>>> prompt   = '. $prompt_flg ."\n" ;
   # die 'exit'."\n\n" ;
   my $Lda = Ora_LDA::ora_LDA( $userid ) ;
   if( $Lda )
     {
      Ora_DIFF2::set_owner_default( $Lda ) ; # set default OWNER from session USER

      Ora_DIFF2::print_sql_diff( $fce,            # Fce=1 => sql KEY CNT
                                 $prompt_flg,     # prompt_flg
                                 $Lda,            # Oracle logon data area
                                 $tab1,           # table_1 name
                                 $tab2,           # table_2 name (undef => table_1)
                                 $q1,             # q_1 (filter for table_1)
                                 $q2,             # q_2 (filter for table_2)
                                 $ra_cols_key_in, # key columns
                                 (($fce == 1) ? $ra_cols_add_in : $ra_cols_omt_in),
                                 $ra_cols_dif_in
                               ) ;
      $Lda->disconnect() or warn "Disconnect: $DBI::errstr\n\n" ;
     }
  }

# --- END ---
