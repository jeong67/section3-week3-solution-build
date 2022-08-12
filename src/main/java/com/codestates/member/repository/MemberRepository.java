package com.codestates.member.repository;

import com.codestates.member.entity.Member;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface MemberRepository extends R2dbcRepository<Member, Long> {  // (1)
    Mono<Member> findByEmail(String email);    // (2)
    Flux<Member> findAllBy(Pageable pageable); // (3)
}