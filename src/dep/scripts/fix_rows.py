import os as os
from glob import glob

HEADER = ['record_type', 'type', 'time', 'project', 'collector', 
          'router', 'router_ip', 'peer_asn', 'peer_address', 
          'prefix', 'next_hop', 'as_path', 'communities']
HEADER_STR = ','.join(HEADER)


def fix_rows(csv_in, dir_out='fixed_csv'):
    print(f'Fixing {csv_in}...')
    
    os.makedirs(dir_out, exist_ok=True)
    csv_out = os.path.join(dir_out, csv_in)

    with open(csv_in, 'r') as file_in:
        with open(csv_out, 'w') as file_out:
            line_counter = 0
            
            while True:
                line = file_in.readline()
         
                if line_counter > 0 and HEADER_STR in line:
                    print(line_counter, line)
                    print('WARNING: Header in rows!')
                    continue

                if line == '':
                    print('End of file!')
                    break
                
                file_out.writelines([line])
                line_counter += 1

    print(f'{csv_in} fixed!')

def main():
    csv_ins = glob('./*.csv')

    for csv_in in csv_ins:
        fix_rows(csv_in) 


if __name__ == '__main__':
    main()
