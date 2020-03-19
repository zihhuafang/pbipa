
######################
### Read tracking. ###
######################

    call build_contig_db {
        input:
            p_ctg_fasta = assemble.p_ctg_fasta,
            a_ctg_fasta = assemble.a_ctg_fasta,
            db_prefix = reads_db_prefix,
            config_sh_fn = generate_config.config_sh_fn,
    }

    call map_all_prepare {
        input:
            seqdb = build_db.seqdb,
            config_sh_fn = generate_config.config_sh_fn,
    }

    Array[String] blocks_map_all = read_lines(map_all_prepare.blocks_fofn)

    scatter (block_id in blocks_map_all) {
        call map_all_run {
            input:
                query_seqdb = build_db.seqdb,
                query_seeddb = build_db.seeddb,
                query_seqdb_seqs = build_db.seqdb_seqs,
                query_seeddb_seeds = build_db.seeddb_seeds,
                target_seqdb = build_contig_db.seqdb,
                target_seeddb = build_contig_db.seeddb,
                target_seqdb_seqs = build_contig_db.seqdb_seqs,
                target_seeddb_seeds = build_contig_db.seeddb_seeds,
                config_sh_fn = generate_config.config_sh_fn,
                num_threads = nproc,
                block_id = block_id,
        }
    }

    call map_all_merge {
        input:
            in_fns = map_all_run.out_m4,
            config_sh_fn = generate_config.config_sh_fn,
            num_threads = nproc,
    }

task build_contig_db {
    input {
        File p_ctg_fasta = "p_ctg.fasta"
        File a_ctg_fasta = "a_ctg.fasta"
        File config_sh_fn
        String db_prefix
    }
    command {
        set -e
        input_p_ctg_fasta="${p_ctg_fasta}" \
        input_a_ctg_fasta="${a_ctg_fasta}" \
        params_db_prefix="${db_prefix}" \
        params_config_sh_fn="${config_sh_fn}" \
            time ipa2-task build_contig_db
    }
    output {
        File seqdb = "${db_prefix}.seqdb"
        File seeddb = "${db_prefix}.seeddb"
        File seqdb_seqs = "${db_prefix}.seqdb.0.seq"
        File seeddb_seeds = "${db_prefix}.seeddb.0.seeds"
    }
}

task map_all_prepare {
    input {
        String seqdb
        File config_sh_fn
    }
    command {
        input_db="${seqdb}" \
        output_blocks="./blocks" \
        params_config_sh_fn="${config_sh_fn}" \
            time ipa2-task map_all_prepare
    }
    output {
        # defer reading this until main workflow resumes (AWS workaround)
        File blocks_fofn = "blocks"
    }
}

task map_all_run {
    input {
        File query_seqdb
        File query_seeddb
        File query_seqdb_seqs
        File query_seeddb_seeds
        File target_seqdb
        File target_seeddb
        File target_seqdb_seqs
        File target_seeddb_seeds
        File config_sh_fn
        Int num_threads
        String block_id
    }
    command {
        input_target_seqdb="${target_seqdb}" \
        input_query_seqdb="${query_seqdb}" \
        params_query_block_id=${block_id} \
        params_num_threads=${num_threads} \
        params_config_sh_fn="${config_sh_fn}" \
            time ipa2-task map_all_run
    }
    runtime {
        cpu: num_threads
    }
    output {
        File out_m4 = "mapped.m4"
    }
}

task map_all_merge {
    input {
        Array[File] in_fns
        File config_sh_fn
        Int num_threads
    }
    command {
        set -vex

        echo ${sep=' ' in_fns} | xargs -n 1 > merged.fofn

        input_fofn=./merged.fofn \
        params_num_threads=${num_threads} \
        params_config_sh_fn="${config_sh_fn}" \
            time ipa2-task map_all_merge
    }
    output {
        File merged_mapped = "mapped.merged.m4"
        File read_to_contig = "read_to_contig.csv"
    }
}
